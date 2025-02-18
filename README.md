# Namibia Real-time Election Dashboard

This project is a real-time election dashboard designed to monitor and visualize election data for Namibia. It integrates various technologies such as Kafka, PostgreSQL, Spark, and Streamlit to provide a comprehensive view of election results, voter turnout, and candidate performance.

## Features

- **Real-time Data Processing**: Utilizes Apache Kafka for real-time data streaming and processing.
- **Data Storage**: PostgreSQL is used to store voter, candidate, and vote data.
- **Data Aggregation**: Apache Spark processes and aggregates data to provide insights such as votes per candidate, voter turnout by region, and voter turnout by constituency.
- **Visualization**: Streamlit is used to create an interactive and real-time dashboard for visualizing election data.

## Technologies Used

- **Apache Kafka**: For real-time data streaming.
- **PostgreSQL**: For persistent data storage.
- **Apache Spark**: For data processing and aggregation.
- **Streamlit**: For building the real-time dashboard.
- **Docker**: For containerizing the services.

## Project Structure

```
namibia-election-dashboard/
│
├── docker-compose.yml          # Docker Compose file to set up the environment
├── main.py                     # Main script to generate and process voter and candidate data
├── consumer.py                 # Kafka consumer to process messages and update the database
├── dashboard.py                # Streamlit dashboard script
├── spark_streaming.py          # Spark streaming job to aggregate data
├── README.md                   # Project README file
└── requirements.txt            # Python dependencies
```

## Getting Started

### Prerequisites

- Docker
- Docker Compose

### Installation

#### Clone the repository

```bash
git clone https://github.com/Tileni97/Namibia-Real-Dashboard-Elections.git
cd namibia-election-dashboard
```

#### Build and start the Docker containers

```bash
docker-compose up --build
```

#### Run the main script to generate data

```bash
docker-compose exec spark-master python main.py
```

#### Start the Spark streaming job

```bash
docker-compose exec spark-master python spark_streaming.py
```

#### Run the Streamlit dashboard

```bash
docker-compose exec spark-master streamlit run dashboard.py
```

### Access the dashboard

Open your browser and navigate to [http://localhost:8501](http://localhost:8501) to view the real-time election dashboard.

## Usage

- **Dashboard**: The Streamlit dashboard provides real-time updates on election results, including leading candidates, voter turnout by region, and voter turnout by constituency.
- **Data Generation**: The `main.py` script generates voter and candidate data, which is then processed by Kafka and stored in PostgreSQL.
- **Data Aggregation**: The `spark_streaming.py` script aggregates the data and publishes the results to Kafka topics, which are then consumed by the dashboard.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request with your changes.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Apache Kafka
- PostgreSQL
- Apache Spark
- Streamlit
- Docker

## Contact

For any questions or feedback, please reach out to [tilenihango@gmail.com](mailto:tilenihango@gmail.com).

