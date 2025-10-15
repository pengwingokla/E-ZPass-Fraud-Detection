# E-ZPass Fraud Detection System

A modern fraud detection system using **Azure OpenAI** and **React** dashboard.

## 📁 Simple Project Structure

```
ezpass-fraud-detection/
├── backend/                    # Node.js API Server
│   ├── services/               # Business logic services
│   │   ├── fraud-detector.js   # TODO: Azure OpenAI integration
│   │   ├── database.js         # TODO: Azure SQL operations  
│   │   └── csv-processor.js    # TODO: File processing
│   ├── routes/
│   │   └── api.js              # TODO: REST API endpoints
│   ├── server.js               # Express server
│   ├── package.json            # Dependencies
│   └── .env.example            # Environment template
│
├── frontend/                   # React Dashboard  
│   ├── src/
│   │   ├── components/         # TODO: React components
│   │   ├── pages/              # TODO: Dashboard pages
│   │   ├── services/           # TODO: API calls
│   │   └── types/              # TODO: TypeScript types
│   ├── package.json            # Frontend dependencies
│   └── vite.config.ts          # Vite config
│
└── README.md                   # This file
```

## � Quick Start

### Backend Setup
```bash
cd backend
npm install
cp .env.example .env
# Add your Azure credentials to .env
npm start
```

### Frontend Setup
```bash
cd frontend
npm install
npm run dev
```

## 🔑 Required Azure Services

1. **Azure OpenAI** - For GPT-4 fraud analysis
2. **Azure SQL Database** - For storing transactions
3. **Email Service** - For fraud alerts

## 💰 Monthly Cost: ~$31
- Azure App Service: $13
- Azure SQL Database: $5  
- Azure OpenAI: $10
- Misc: $3

## 🎯 Features

- Upload CSV files with E-ZPass transactions
- AI-powered fraud detection using GPT-4
- Real-time dashboard with fraud statistics
- Email alerts for high fraud rates
- Simple REST API

## 📊 Tech Stack

- **Backend**: Node.js, Express
- **Frontend**: React, Vite, TypeScript
- **AI**: Azure OpenAI (GPT-4)
- **Database**: Azure SQL Database
- **Hosting**: Azure App Service

## 📋 Project Features
- ✅ Comprehensive design diagram illustrating the implementation process
- ✅ Documented functional use cases featuring proposed enhancements and requirements
- ✅ Detailed business rules capturing new fraud detection criteria
- ✅ Test cases for validating key system functionalities
- ✅ Complete source code for the developed solution

## 🏗️ Architecture
- **Frontend**: React.js with modern UI/UX
- **Backend API**: Node.js with Express
- **ML/Analytics**: Python (scikit-learn, pandas, numpy)
- **Database**: PostgreSQL (structured data)
- **Cloud**: Azure/AWS/GCP (pending NJIT credits confirmation)
- **Version Control**: GitHub

## 📁 Project Structure
```
ezpass-fraud-detection/
├── 📚 docs/                    # All project documentation
│   ├── design/                 # System design diagrams
│   ├── business-rules/         # Business rules documentation
│   ├── use-cases/              # Functional use cases
│   └── api/                    # API documentation
├── 🖥️ frontend/                # React.js application
├── ⚡ backend/                 # Node.js API server
├── 🧠 ml-analytics/            # Python ML models and analytics
│   ├── models/                 # Trained ML models
│   ├── data/                   # ML-specific data processing
│   └── notebooks/              # Jupyter notebooks for analysis
├── 🧪 tests/                   # Test suites
│   ├── unit/                   # Unit tests
│   ├── integration/            # Integration tests
│   └── e2e/                    # End-to-end tests
├── 🚀 deployment/              # Deployment configurations
│   ├── docker/                 # Docker configurations
│   └── cloud/                  # Cloud deployment scripts
├── 📊 data/                    # Data management
│   ├── raw/                    # Raw E-ZPass data
│   ├── processed/              # Cleaned and processed data
│   └── samples/                # Sample data for testing
└── 🔧 scripts/                 # Utility scripts
```

## 🎯 Key Features
- **Fraud Detection**: AI-powered pattern recognition for suspicious activities
- **Real-time Monitoring**: Continuous monitoring of E-ZPass transactions
- **User-friendly Dashboard**: Step-by-step process to identify inconsistencies
- **Automated Flagging**: Flag fraud based on business rules and data patterns
- **Compliance Support**: Support investigative workflows and compliance reporting

## 🚀 Getting Started
See individual component READMEs in their respective directories for setup instructions.

## 👥 Team
NJIT Capstone Team - 4 Members
Project Duration: 2 Months

## 📝 License
This project is developed as part of NJIT Capstone Project with NJ Courts.

