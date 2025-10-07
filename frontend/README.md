# 🛡️ EZ Pass Fraud Detection Dashboard

A modern, professional fraud detection dashboard for New Jersey Courts built with React, featuring real-time transaction monitoring and analytics.

![React](https://img.shields.io/badge/React-18.2.0-61dafb)
![Chart.js](https://img.shields.io/badge/Chart.js-4.4.0-ff6384)
![Tailwind](https://img.shields.io/badge/Tailwind-3.3.0-38bdf8)

## ✨ Features

### 📊 Dashboard View
- **Statistics Cards** - Total Alerts (YTD), Potential Loss (YTD), Detected Frauds (Current Month)
- **Transaction Analysis** - Bar chart showing transactions vs fraud alerts over 6 months
- **Recent Flagged Transactions** - Live feed of flagged and investigating transactions
- **Fraud by Category** - Doughnut chart (Toll Evasion, Card Skimming, Account Takeover)
- **Threat Severity** - Doughnut chart (High, Medium, Low)

### � Raw Data View
- **Real-time Search** - Search across transaction IDs and categories
- **Status Filters** - Filter by All, Flagged, Investigating, or Resolved
- **Interactive Status Dropdown** - Change transaction status directly from the table
- **Transaction Details** - Tag/Plate Number, Transaction Date, Agency, Entry/Exit info, Amount, Category
- **Color-coded Badges** - PANYNJ (blue), NJTA (purple), status-based colors
- **Pagination** - Navigate through transaction data

### � Design
- **Teal Theme** - New Jersey Courts branded color scheme
- **Dark Mode** - Professional dark interface with teal accents
- **Responsive Layout** - Works on all screen sizes
- **Smooth Animations** - Clean transitions and hover effects
- **Glass Morphism** - Modern frosted glass card effects

## 🚀 Quick Start

```bash
# Install dependencies
npm install

# Start development server
npm start
```

App runs at [http://localhost:3000](http://localhost:3000)

### Build for Production

```bash
npm run build
```

## 📁 Project Structure

```
frontend/
├── public/
│   └── index.html
├── src/
│   ├── App.js              # Main component with Dashboard & Data views
│   ├── index.js            # Entry point
│   └── index.css           # Global styles
├── package.json
├── tailwind.config.js
└── postcss.config.js
```

## 🛠️ Technologies

- **React 18.2** - Component-based UI
- **Chart.js 4.4** - Data visualization
- **Tailwind CSS 3.3** - Utility-first styling
- **Inter Font** - Professional typography

## 📊 Data Columns

**Transaction Table:**
- Tag/Plate Number
- Transaction Date
- Agency (PANYNJ/NJTA)
- Entry Time & Plaza
- Exit Time & Plaza
- Amount
- Status (Dropdown: Flagged/Investigating/Resolved)
- Category (Toll Evasion/Card Skimming/Account Takeover)

## � Key Features

✅ **Interactive Status Management** - Update transaction status via dropdown  
✅ **Real-time Filtering** - Search and filter transactions instantly  
✅ **Recent Alerts Feed** - Quick view of flagged transactions  
✅ **Visual Analytics** - Charts for category and severity distribution  
✅ **NJ Courts Branding** - Teal color theme matching official branding  
✅ **Clean UI** - No clutter, straight to the data  

## 📱 Responsive

- Mobile: < 640px
- Tablet: 640px - 1024px
- Desktop: > 1024px

## 🎨 Color Scheme

- **Primary**: Teal (`#14b8a6`) - Main theme
- **Success**: Emerald (`#22c55e`) - Resolved status
- **Warning**: Amber (`#f59e0b`) - Flagged status
- **Danger**: Red (`#ef4444`) - Investigating status
- **Agency PANYNJ**: Blue (`#3b82f6`)
- **Agency NJTA**: Purple (`#a855f7`)

---

Built for New Jersey Courts EZ Pass Fraud Detection System