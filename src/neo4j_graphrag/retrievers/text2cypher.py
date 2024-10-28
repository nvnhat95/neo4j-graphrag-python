# This modifies the original code from
# https://github.com/neo4j/neo4j-graphrag-python/blob/main/src/neo4j_graphrag/retrievers/text2cypher.py
# to feed list of current named entities in KG to LLM prompt in order to retrieve data more efficiently
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional

import neo4j
from neo4j.exceptions import CypherSyntaxError, DriverError, Neo4jError
from neo4j_graphrag.exceptions import (
    RetrieverInitializationError,
    SchemaFetchError,
    SearchValidationError,
    Text2CypherRetrievalError,
)
from neo4j_graphrag.generation.prompts import PromptTemplate
from neo4j_graphrag.llm import LLMInterface
from neo4j_graphrag.retrievers.base import Retriever
from neo4j_graphrag.schema import get_schema
from neo4j_graphrag.types import (
    LLMModel,
    Neo4jDriverModel,
    Neo4jSchemaModel,
    RawSearchResult,
    RetrieverResultItem,
    Text2CypherRetrieverModel,
    Text2CypherSearchModel,
)
from pydantic import ValidationError
from neo4j_graphrag.experimental.components.entity_getter import (
    get_relevant_entities_by_label,
)

logger = logging.getLogger(__name__)


class Text2CypherTemplate(PromptTemplate):
    DEFAULT_TEMPLATE = """Task: Generate a Cypher statement for querying a Neo4j graph database from a user input.

Schema:
{schema}

List of relevant entities in knowledge graph:
{current_entities}

Input:
{query_text}

Do not use any properties or relationships not included in the schema.
Do not include triple backticks ``` or any additional text except the generated Cypher statement in your response.
There might be multiple relationships between two entities.
You can write a simple Cypher query to retrieve redundant data instead of using a complex query.

Cypher query:
"""
    EXPECTED_INPUTS = ["query_text"]

    def format(
        self,
        schema: Optional[str] = None,
        examples: Optional[str] = None,
        query_text: str = "",
        query: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        if query is not None:
            if query_text:
                warnings.warn(
                    "Both 'query' and 'query_text' are provided, 'query_text' will be used.",
                    DeprecationWarning,
                    stacklevel=2,
                )
            elif isinstance(query, str):
                warnings.warn(
                    "'query' is deprecated and will be removed in a future version, please use 'query_text' instead.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                query_text = query

        return super().format(
            query_text=query_text, schema=schema, examples=examples, **kwargs
        )


class Text2CypherRetriever(Retriever):
    """
    Allows for the retrieval of records from a Neo4j database using natural language.
    Converts a user's natural language query to a Cypher query using an LLM,
    then retrieves records from a Neo4j database using the generated Cypher query

    Args:
        driver (neo4j.driver): The Neo4j Python driver.
        llm (neo4j_graphrag.generation.llm.LLMInterface): LLM object to generate the Cypher query.
        neo4j_schema (Optional[str]): Neo4j schema used to generate the Cypher query.
        examples (Optional[list[str], optional): Optional user input/query pairs for the LLM to use as examples.
        custom_prompt (Optional[str]): Optional custom prompt to use instead of auto generated prompt. Will not include the neo4j_schema or examples args, if provided.

    Raises:
        RetrieverInitializationError: If validation of the input arguments fail.
    """

    def __init__(
        self,
        driver: neo4j.Driver,
        llm: LLMInterface,
        neo4j_schema: Optional[str] = None,
        examples: Optional[list[str]] = None,
        result_formatter: Optional[
            Callable[[neo4j.Record], RetrieverResultItem]
        ] = None,
        custom_prompt: Optional[str] = None,
    ) -> None:
        try:
            driver_model = Neo4jDriverModel(driver=driver)
            llm_model = LLMModel(llm=llm)
            neo4j_schema_model = (
                Neo4jSchemaModel(neo4j_schema=neo4j_schema) if neo4j_schema else None
            )
            validated_data = Text2CypherRetrieverModel(
                driver_model=driver_model,
                llm_model=llm_model,
                neo4j_schema_model=neo4j_schema_model,
                examples=examples,
                result_formatter=result_formatter,
                custom_prompt=custom_prompt,
            )
        except ValidationError as e:
            raise RetrieverInitializationError(e.errors()) from e

        super().__init__(validated_data.driver_model.driver)
        self.driver = validated_data.driver_model.driver
        self.llm = validated_data.llm_model.llm
        self.examples = validated_data.examples
        self.result_formatter = validated_data.result_formatter
        self.custom_prompt = validated_data.custom_prompt
        # try:
        #     if (
        #         not validated_data.custom_prompt
        #     ):  # don't need schema for a custom prompt
        #         self.neo4j_schema = (
        #             validated_data.neo4j_schema_model.neo4j_schema
        #             if validated_data.neo4j_schema_model
        #             else get_schema(validated_data.driver_model.driver)
        #         )
        #     else:
        #         self.neo4j_schema = ""

        self.neo4j_schema = neo4j_schema

        # except (Neo4jError, DriverError) as e:
        #     error_message = getattr(e, "message", str(e))
        #     raise SchemaFetchError(
        #         f"Failed to fetch schema for Text2CypherRetriever: {error_message}"
        #     ) from e

    def get_search_results(
        self, query_text: str, prompt_params: Optional[Dict[str, Any]] = None
    ) -> RawSearchResult:
        """Converts query_text to a Cypher query using an LLM.
           Retrieve records from a Neo4j database using the generated Cypher query.

        Args:
            query_text (str): The natural language query used to search the Neo4j database.
            prompt_params (Dict[str, Any]): additional values to inject into the custom prompt, if it is provided. Example: {'schema': 'this is the graph schema'}

        Raises:
            SearchValidationError: If validation of the input arguments fail.
            Text2CypherRetrievalError: If the LLM fails to generate a correct Cypher query.

        Returns:
            RawSearchResult: The results of the search query as a list of neo4j.Record and an optional metadata dict
        """
        try:
            validated_data = Text2CypherSearchModel(query_text=query_text)
        except ValidationError as e:
            raise SearchValidationError(e.errors()) from e

        prompt_template = Text2CypherTemplate(template=self.custom_prompt)

        prompt_params = prompt_params or {}

        # print("=== schema", self.neo4j_schema)

        params_to_use = {
            "schema": self.neo4j_schema,  # prompt_params.get("schema") or self.neo4j_schema,
            "query_text": validated_data.query_text,
            "current_entities": prompt_params.get("current_entities")
            or get_relevant_entities_by_label(self.driver, validated_data.query_text),
        }

        print("=== current_entities", params_to_use["current_entities"])

        prompt = prompt_template.format(**params_to_use)

        logger.debug("Text2CypherRetriever prompt: %s", prompt)

        try:
            print("=== Text2CypherRetriever prompt", prompt)
            llm_result = self.llm.invoke(prompt)
            t2c_query = llm_result.content
            print("=== Text2CypherRetriever t2c_query", t2c_query)
            logger.debug("Text2CypherRetriever Cypher query: %s", t2c_query)
            records, _, _ = self.driver.execute_query(query_=t2c_query)
        except CypherSyntaxError as e:
            raise Text2CypherRetrievalError(
                f"Failed to get search result: {e.message}"
            ) from e

        return RawSearchResult(
            records=records,
            metadata={
                "cypher": t2c_query,
            },
        )
