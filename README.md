# E-ZGuard: E-ZPass Fraud Detection System

E-ZGuard is an automated fraud detection and analytics platform designed to identify suspicious activity within E-ZPass toll transaction data. Built for the [New Jersey Courts](https://www.njcourts.gov/) system, it combines machine learning, automated data pipelines, and an intuitive dashboard to help investigators detect anomalies quickly and accurately.

## Key Features

- **Automated Fraud Detection**: Uses machine learning models (Isolation Forest) to automatically identify anomalous transaction patterns
- **Real-Time Data Processing**: Automated pipeline that ingests, transforms, and enriches transaction data
- **Interactive Dashboard**: Modern web-based dashboard with visualizations, filtering, and secure authentication
- **Scalable Architecture**: Built on Google Cloud Platform with BigQuery, Vertex AI, Airflow, and dbt

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.10+
- Node.js 16+ and npm
- Google Cloud Platform account with:
  - Cloud Storage API
  - BigQuery API
  - Vertex AI API

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd E-ZPass-Fraud-Detection
   ```

2. **Configure environment**
   ```bash
   cp env.template .env
   # Edit .env with your configuration
   ```

3. **Set up GCP credentials**
   - Create a service account with required permissions
   - Place the JSON key in `src/airflow/config/gcp-key.json`

4. **Start services with Docker**
   ```bash
   docker-compose build
   docker-compose up airflow-init
   docker-compose up -d
   ```

5. **Access the services**
   - Airflow UI: http://localhost:8080 (username: `airflow`, password: `airflow`)
   - Backend API: http://localhost:5000
   - Frontend: http://localhost:3000

For detailed setup instructions, see the [Setup Manual](docs/setup-manual.html).

## Documentation

- **[Setup Manual](docs/setup-manual.html)**: Comprehensive installation and configuration guide
- **[API Reference](docs/setup-manual.html#api-overview)**: Complete API endpoint documentation
- **[Project Documentation](https://drive.google.com/drive/folders/1k9JhQVRJAZEYMZTjLwgfGh1DHFsIAv2a?usp=sharing)**: Google Drive folder with project description, scope, and architecture planning

## Technology Stack

- **Backend**: Python, Flask, BigQuery
- **Frontend**: React, Tailwind CSS
- **Data Processing**: Apache Airflow, dbt
- **Machine Learning**: Vertex AI, scikit-learn
- **Cloud**: Google Cloud Platform (GCS, BigQuery, Vertex AI)
- **Containerization**: Docker, Docker Compose

## Business Value

- **Saves 90% Review Time**: Processes 18,000+ transactions in under 20 minutes
- **Reduces Error**: Flags ~1% of records as anomalies with fraud scores and risk levels
- **Detects Patterns Human Miss**: Evaluates 30+ engineered features including impossible travel speeds and route inconsistencies

## Testing

Run backend tests:
```bash
cd backend
pytest tests/
```

Run frontend tests:
```bash
cd frontend
npm test
```

## Contributing

1. Create a feature branch from `main`
2. Make your changes
3. Ensure tests pass
4. Submit a pull request

## License

_To be determined_

## Contributors

- **Meer Modi** - Project Manager
- **Chloe Nguyen** - Machine Learning Engineer
- **Eric Liu** - Data Scientist
- **Xitlaly Prado**  - Full-stack Engineer
- **Joshua DeMarco** - Security Engineer

## Related Links

- [New Jersey Courts](https://www.njcourts.gov/)
- [Setup Manual](docs/setup-manual.html)
- [API Reference](docs/setup-manual.html#api-overview)
- [Google Slides](https://docs.google.com/presentation/d/1pgGwSzpV89Q9UtXA7dg9pa52v4l-0iMfACFaLnFE840/edit?usp=sharing)
- [Project Documentation](https://drive.google.com/drive/folders/1k9JhQVRJAZEYMZTjLwgfGh1DHFsIAv2a?usp=sharing)
