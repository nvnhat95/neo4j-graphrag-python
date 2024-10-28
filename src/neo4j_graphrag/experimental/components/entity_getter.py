from difflib import SequenceMatcher
from typing import Union

import neo4j

from neo4j_graphrag.experimental.pipeline.component import Component, DataModel


class NamedEntities(DataModel):
    entities_by_label: dict[str, list[str]]


def get_entities_by_label(driver, ignore_chunks=True):
    def get_entities_by_label_tx(tx):
        # Get all labels
        labels = tx.run("CALL db.labels() YIELD label RETURN label")
        entities_by_label = {}

        # For each label, get entities
        for record in list(labels):
            label = record["label"]
            if label in ["__KGBuilder__", "__Entity__"]:
                continue
            if label == "Chunk" and ignore_chunks:
                continue
            entities = tx.run(f"MATCH (n:{label}) RETURN n.name AS name")
            entity_names = set(
                [entity["name"] for entity in entities if entity["name"] is not None]
            )
            if entity_names:
                entities_by_label[label] = list(entity_names)

        return entities_by_label

    with driver.session() as session:
        entities_by_label = session.execute_read(get_entities_by_label_tx)
    return entities_by_label


def longest_subsequence_ratio(entity: str, query: str) -> float:
    """calculate the similarity score between the keyword (entity) and the query

    Go through each possible substring (span) of the query,
    calculate the longest common subsequence (LCS) between the entity and each span,
    and then determine the ratio of the LCS length
    to the maximum length of either the entity or the query.
    """
    max_ratio = 0
    entity_len = len(entity.replace(" ", ""))
    query_words = query.split()

    # Generate all possible spans that are contiguous sequences of words
    for i in range(len(query_words)):
        for j in range(i + 1, len(query_words) + 1):
            span = "".join(query_words[i:j])

            # Calculate the longest subsequence length using SequenceMatcher
            matcher = SequenceMatcher(None, entity, span)
            lcs_length = sum(block.size for block in matcher.get_matching_blocks())
            # Calculate the ratio of LCS length to the max length of entity or query
            ratio = lcs_length / max(entity_len, len(span))

            # Update max_ratio if the current ratio is higher
            max_ratio = max(max_ratio, ratio)

    return max_ratio


def find_most_similar_entities(
    query: str,
    entities: list[str],
    min_score: float = 0.1,
) -> list[tuple[str, float]]:
    """
    Finds the most similar entities based on the length of the longest common substring.
    Returns the top_k entities with the longest common substrings.

    :param query: str, the user query
    :param entities: list of str, the list of named entities to compare
    :return: list of tuples (entity, LCS_length), sorted by LCS length in descending order
    """
    # Calculate LCS length for each entity
    entity_scores = [
        (entity, longest_subsequence_ratio(query, entity)) for entity in entities
    ]

    # Sort by LCS length in descending order and return the top_k entities
    sorted_entities = sorted(entity_scores, key=lambda x: x[1], reverse=True)

    return [entity for entity, score in sorted_entities if score >= min_score]


def get_relevant_entities_by_label(driver, query: str, ignore_chunks=True):
    all_entities = get_entities_by_label(driver, ignore_chunks)
    relevant_entities = {}
    for label, entities in all_entities.items():
        relevant_ents = find_most_similar_entities(query, entities, min_score=0.1)
        if relevant_ents:
            relevant_entities[label] = relevant_ents
    return all_entities


class GetEntitiesComponent(Component):
    def __init__(self, driver: Union[neo4j.Driver, neo4j.AsyncDriver]):
        self.driver = driver

    async def run(self) -> NamedEntities:
        entities_by_label = get_entities_by_label(self.driver)
        print("=== entities_by_label", entities_by_label)
        named_entities = NamedEntities(entities_by_label=entities_by_label)
        return named_entities
