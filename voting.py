import random
import time
from datetime import datetime

import psycopg2
import simplejson as json
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer

from main import delivery_report

conf = {
    "bootstrap.servers": "localhost:9092",
}

consumer = Consumer(
    conf
    | {
        "group.id": "voting-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
)

producer = SerializingProducer(conf)


def consume_messages():
    result = []
    consumer.subscribe(["candidates_topic"])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                result.append(json.loads(msg.value().decode("utf-8")))
                if len(result) == 3:
                    return result

            # time.sleep(5)
    except KafkaException as e:
        print(e)


if __name__ == "__main__":
    conn = psycopg2.connect(
        "host=localhost dbname=voting user=postgres password=postgres"
    )
    cur = conn.cursor()

    # Fetch candidates from the database
    candidates_query = cur.execute(
        """
        SELECT row_to_json(t)
        FROM (
            SELECT * FROM candidates
        ) t;
    """
    )
    candidates = cur.fetchall()
    candidates = [candidate[0] for candidate in candidates]
    if len(candidates) == 0:
        raise Exception("No candidates found in database")
    else:
        print(candidates)

    consumer.subscribe(["voters_topic"])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                voter = json.loads(msg.value().decode("utf-8"))
                # Ensure the voter can only vote once per election type
                cur.execute(
                    """
                    SELECT election_type FROM votes WHERE voter_id = %s
                """,
                    (voter["voter_id"],),
                )
                existing_votes = cur.fetchall()
                existing_election_types = [vote[0] for vote in existing_votes]

                # Filter candidates based on election type
                available_election_types = ["President", "National Assembly"]
                for election_type in available_election_types:
                    if election_type not in existing_election_types:
                        # Choose a candidate for the current election type
                        candidates_for_election = [
                            candidate
                            for candidate in candidates
                            if candidate["election_type"] == election_type
                        ]
                        if not candidates_for_election:
                            print(
                                f"No candidates found for election type: {election_type}"
                            )
                            continue

                        chosen_candidate = random.choice(candidates_for_election)
                        vote = (
                            voter
                            | chosen_candidate
                            | {
                                "voting_time": datetime.utcnow().strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                                "vote": 1,
                                "election_type": election_type,
                            }
                        )

                        try:
                            print(
                                f"User {vote['voter_id']} is voting for candidate: {vote['candidate_id']} in {election_type} election"
                            )
                            cur.execute(
                                """
                                INSERT INTO votes (voter_id, candidate_id, voting_time, election_type)
                                VALUES (%s, %s, %s, %s)
                            """,
                                (
                                    vote["voter_id"],
                                    vote["candidate_id"],
                                    vote["voting_time"],
                                    vote["election_type"],
                                ),
                            )

                            conn.commit()

                            producer.produce(
                                "votes_topic",
                                key=vote["voter_id"],
                                value=json.dumps(vote),
                                on_delivery=delivery_report,
                            )
                            producer.poll(0)
                        except Exception as e:
                            print(f"Error: {e}")
                            conn.rollback()
                            continue

            time.sleep(0.2)
    except KafkaException as e:
        print(e)
