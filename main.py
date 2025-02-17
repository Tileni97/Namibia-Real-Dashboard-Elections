import random
import psycopg2
import requests
import simplejson as json
from confluent_kafka import SerializingProducer

BASE_URL = "https://randomuser.me/api/?nat=gb"
# Updated list of Namibian political parties
PARTIES = [
    "SWAPO Party of Namibia",
    "Popular Democratic Movement (PDM)",
    "Landless People's Movement (LPM)",
    "Independent Patriots for Change (IPC)",
    "United Democratic Front of Namibia (UDF)",
    "Republican Party of Namibia (RP)",
    "All People's Party (APP)",
    "Congress of Democrats (CoD)",
    "Namibia Economic Freedom Fighters (NEFF)",
    "Rally for Democracy and Progress (RDP)",
    "Affirmative Repositioning (AR)",
    "United Namibians Party (UNP)",
]
random.seed(42)


def generate_voter_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        user_data = response.json()["results"][0]
        return {
            "voter_id": user_data["login"]["uuid"],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data["dob"]["date"],
            "gender": user_data["gender"],
            "nationality": "Namibian",  # Default to Namibian
            "registration_number": user_data["login"]["username"],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data["location"]["city"],
                "state": user_data["location"]["state"],
                "country": "Namibia",  # Default to Namibia
                "postcode": user_data["location"]["postcode"],
            },
            "email": user_data["email"],
            "phone_number": user_data["phone"],
            "cell_number": user_data["cell"],
            "picture": user_data["picture"]["large"],
            "registered_age": user_data["registered"]["age"],
            "region": random.choice(
                [
                    "Khomas",
                    "Erongo",
                    "Oshana",
                    "Omusati",
                    "Oshikoto",
                    "Ohangwena",
                    "Kavango East",
                    "Kavango West",
                    "Zambezi",
                    "Otjozondjupa",
                    "Omaheke",
                    "Kunene",
                    "Hardap",
                    "ǁKaras",
                ]
            ),  # Random region
            "constituency": f"Constituency {random.randint(1, 121)}",  # Namibia has 121 constituencies
        }
    else:
        return "Error fetching data"


def generate_candidate_data(candidate_number, total_parties):
    response = requests.get(
        BASE_URL + "&gender=" + ("female" if candidate_number % 2 == 1 else "male")
    )
    if response.status_code == 200:
        user_data = response.json()["results"][0]

        return {
            "candidate_id": user_data["login"]["uuid"],
            "candidate_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "party_affiliation": PARTIES[candidate_number % total_parties],
            "biography": "A brief bio of the candidate.",
            "campaign_platform": "Key campaign promises or platform.",
            "photo_url": user_data["picture"]["large"],
            "election_type": random.choice(
                ["President", "National Assembly"]
            ),  # Random election type
            "constituency": f"Constituency {random.randint(1, 121)}",  # For National Assembly candidates
            "is_independent": random.choice(
                [True, False]
            ),  # Randomly assign independence
            "supporters_required": (
                500 if random.choice([True, False]) else None
            ),  # For independent presidential candidates
        }
    else:
        return "Error fetching data"


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


# Kafka Topics
voters_topic = "voters_topic"
candidates_topic = "candidates_topic"


def create_tables(conn, cur):
    # Create Candidates Table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255) NOT NULL,
            party_affiliation VARCHAR(255),
            biography TEXT,
            campaign_platform TEXT,
            photo_url TEXT,
            election_type VARCHAR(255) NOT NULL,
            constituency VARCHAR(255),
            is_independent BOOLEAN DEFAULT FALSE,
            supporters_required INT
        )
    """
    )

    # Create Voters Table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255) NOT NULL,
            date_of_birth DATE NOT NULL,
            gender VARCHAR(50),
            nationality VARCHAR(255) DEFAULT 'Namibian',
            registration_number VARCHAR(255) UNIQUE NOT NULL,
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255) DEFAULT 'Namibia',
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER,
            region VARCHAR(255) NOT NULL,
            constituency VARCHAR(255)
        )
    """
    )

    # Create Votes Table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS votes (
            vote_id SERIAL PRIMARY KEY,
            voter_id VARCHAR(255) NOT NULL,
            candidate_id VARCHAR(255) NOT NULL,
            election_type VARCHAR(255) NOT NULL,
            voting_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            vote INT DEFAULT 1,
            FOREIGN KEY (voter_id) REFERENCES voters(voter_id),
            FOREIGN KEY (candidate_id) REFERENCES candidates(candidate_id)
        )
    """
    )

    conn.commit()


def insert_voters(conn, cur, voter):
    cur.execute(
        """
        INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number, address_street, address_city, address_state, address_country, address_postcode, email, phone_number, cell_number, picture, registered_age, region, constituency)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
        (
            voter["voter_id"],
            voter["voter_name"],
            voter["date_of_birth"],
            voter["gender"],
            voter["nationality"],
            voter["registration_number"],
            voter["address"]["street"],
            voter["address"]["city"],
            voter["address"]["state"],
            voter["address"]["country"],
            voter["address"]["postcode"],
            voter["email"],
            voter["phone_number"],
            voter["cell_number"],
            voter["picture"],
            voter["registered_age"],
            voter["region"],
            voter["constituency"],
        ),
    )
    conn.commit()


if __name__ == "__main__":
    conn = psycopg2.connect(
        "host=localhost dbname=voting user=postgres password=postgres"
    )
    cur = conn.cursor()

    producer = SerializingProducer(
        {
            "bootstrap.servers": "localhost:9092",
        }
    )
    create_tables(conn, cur)

    # Get candidates from the database
    cur.execute(
        """
        SELECT * FROM candidates
    """
    )
    candidates = cur.fetchall()
    print(candidates)

    if len(candidates) == 0:
        for i in range(12):  # Generate 12 candidates (one for each party)
            candidate = generate_candidate_data(i, len(PARTIES))
            print(candidate)
            cur.execute(
                """
                INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url, election_type, constituency, is_independent, supporters_required)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    candidate["candidate_id"],
                    candidate["candidate_name"],
                    candidate["party_affiliation"],
                    candidate["biography"],
                    candidate["campaign_platform"],
                    candidate["photo_url"],
                    candidate["election_type"],
                    candidate["constituency"],
                    candidate["is_independent"],
                    candidate["supporters_required"],
                ),
            )
            conn.commit()

    for i in range(1000):  # Generate 1000 voters
        voter_data = generate_voter_data()
        insert_voters(conn, cur, voter_data)

        producer.produce(
            voters_topic,
            key=voter_data["voter_id"],
            value=json.dumps(voter_data),
            on_delivery=delivery_report,
        )

        print("Produced voter {}, data: {}".format(i, voter_data))
        producer.flush()
