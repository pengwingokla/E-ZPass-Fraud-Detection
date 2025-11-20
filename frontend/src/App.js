import React, { useState, useEffect, useRef } from 'react';
import { Chart, registerables } from 'chart.js';
Chart.register(...registerables);



// --- Mock Data ---
const rawData = [
    { 
        id: 'TXN748392', 
        transactionDate: '10/02/2025',
        tagPlate: '6618548668',
        agency: 'PANYNJ',
        entryTime: '19:45:12',
        entryPlaza: 'HOLLAND',
        exitPlaza: 'GWU',
        exitTime: '20:27:03',
        amount: 108.40,
        status: 'Flagged',
        category: 'Toll Evasion', 
        severity: 'Low', 
        riskScore: 35 
    },
    { 
        id: 'TXN748391', 
        transactionDate: '10/01/2025',
        tagPlate: '7729659779',
        agency: 'PANYNJ',
        entryTime: '13:30:45',
        entryPlaza: 'GWU',
        exitPlaza: 'LINCOLN',
        exitTime: '14:15:22',
        amount: 350.00,
        status: 'Investigating',
        category: 'Account Takeover', 
        severity: 'High', 
        riskScore: 92 
    },
    { 
        id: 'TXN748390', 
        transactionDate: '10/01/2025',
        tagPlate: '8830760880',
        agency: 'NJTA',
        entryTime: '08:20:33',
        entryPlaza: 'PARKWAY',
        exitPlaza: 'TURNPIKE',
        exitTime: '09:45:11',
        amount: 85.70,
        status: 'Flagged',
        category: 'Card Skimming', 
        severity: 'Medium', 
        riskScore: 68 
    },
    { 
        id: 'TXN748389', 
        transactionDate: '09/30/2025',
        tagPlate: '9941871991',
        agency: 'PANYNJ',
        entryTime: '16:55:20',
        entryPlaza: 'LINCOLN',
        exitPlaza: 'HOLLAND',
        exitTime: '17:30:45',
        amount: 5.75,
        status: 'Resolved',
        category: 'Toll Evasion', 
        severity: 'Low', 
        riskScore: 28 
    },
    { 
        id: 'TXN748388', 
        transactionDate: '09/29/2025',
        tagPlate: '1052982102',
        agency: 'NJTA',
        entryTime: '21:40:15',
        entryPlaza: 'TURNPIKE',
        exitPlaza: 'PARKWAY',
        exitTime: '22:10:33',
        amount: 8.00,
        status: 'Resolved',
        category: 'Toll Evasion', 
        severity: 'Low', 
        riskScore: 22 
    },
    { 
        id: 'TXN748387', 
        transactionDate: '09/28/2025',
        tagPlate: '2163093213',
        agency: 'PANYNJ',
        entryTime: '10:35:42',
        entryPlaza: 'HOLLAND',
        exitPlaza: 'GWU',
        exitTime: '11:20:18',
        amount: 120.00,
        status: 'Flagged',
        category: 'Card Skimming', 
        severity: 'Medium', 
        riskScore: 71 
    },
    { 
        id: 'TXN748386', 
        transactionDate: '09/27/2025',
        tagPlate: '3274104324',
        agency: 'NJTA',
        entryTime: '07:30:10',
        entryPlaza: 'PARKWAY',
        exitPlaza: 'TURNPIKE',
        exitTime: '08:55:27',
        amount: 980.21,
        status: 'Investigating',
        category: 'Account Takeover', 
        severity: 'High', 
        riskScore: 95 
    },
    { 
        id: 'TXN748385', 
        transactionDate: '09/26/2025',
        tagPlate: '4385215435',
        agency: 'PANYNJ',
        entryTime: '16:10:25',
        entryPlaza: 'GWU',
        exitPlaza: 'LINCOLN',
        exitTime: '16:40:50',
        amount: 15.00,
        status: 'Resolved',
        category: 'Toll Evasion', 
        severity: 'Low', 
        riskScore: 31 
    },
];
const apiUrl = process.env.REACT_APP_API_URL;

const fetchTransactions = async () => {
    try {
        console.log(import.meta.env);
        const response = await fetch(`${apiUrl}/api/transactions`);
        if (!response.ok) throw new Error('Failed to fetch data');
        const { data } = await response.json(); // extract array
        return data || [];
    } catch (err) {
        console.error(err);
        return [];
    }
};

const fetchMonthlyChartData = async () => {
    try {
        const response = await fetch(`${apiUrl}/api/charts/monthly`);
        if (!response.ok) throw new Error('Failed to fetch chart data');
        const { data } = await response.json();
        return data || [];
    } catch (err) {
        console.error(err);
        return [];
    }
};

const fetchCategoryChartData = async () => {
    try {
        const response = await fetch(`${apiUrl}/api/charts/category`);
        if (!response.ok) throw new Error('Failed to fetch category chart data');
        const { data } = await response.json();
        return data || [];
    } catch (err) {
        console.error(err);
        return [];
    }
};

const fetchMetrics = async () => {
    try {
        const response = await fetch(`${apiUrl}/api/metrics`);
        if (!response.ok) throw new Error('Failed to fetch metrics');
        const data = await response.json();
        return data;
    } catch (err) {
        console.error(err);
        return {
            total_transactions: 0,
            total_flagged: 0,
            total_amount: 0,
            total_alerts_ytd: 0,
            detected_frauds_current_month: 0
        };
    }
};

const fetchRecentAlerts = async () => {
    try {
        const response = await fetch(`${apiUrl}/api/transactions/alerts`);
        if (!response.ok) throw new Error('Failed to fetch alerts');
        const { data } = await response.json();
        return data || [];
    } catch (err) {
        console.error(err);
        return [];
    }
};


/*
const recentAlerts = [
    { time: '2 min ago', message: 'High-risk transaction detected', type: 'critical' },
    { time: '5 min ago', message: 'Card skimming pattern identified', type: 'warning' },
    { time: '12 min ago', message: 'Account takeover attempt blocked', type: 'critical' },
];
*/
// --- Icon Components ---
const ShieldIcon = () => (
    <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" />
    </svg>
);

const AlertIcon = () => (
    <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
    </svg>
);

const DollarIcon = () => (
    <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
    </svg>
);

const SearchIcon = () => (
    <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
    </svg>
);

/*
const TrendUpIcon = () => (
    <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
    </svg>
);

const TrendDownIcon = () => (
    <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 17h8m0 0V9m0 8l-8-8-4 4-6-6" />
    </svg>
);
*/
// --- Chart Components ---

const defaultChartOptions = {
    maintainAspectRatio: false,
    responsive: true,
    plugins: {
        legend: {
            labels: {
                color: '#e5e7eb',
                font: { family: "'Inter', sans-serif", size: 12, weight: '500' },
                padding: 15,
                usePointStyle: true,
                pointStyle: 'circle'
            }
        },
        tooltip: {
            backgroundColor: 'rgba(15, 23, 42, 0.95)',
            titleColor: '#f1f5f9',
            bodyColor: '#cbd5e1',
            borderColor: '#334155',
            borderWidth: 1,
            padding: 12,
            displayColors: true,
            boxPadding: 6
        }
    },
    scales: {
        x: {
            ticks: { 
                color: '#94a3b8', 
                font: { family: "'Inter', sans-serif", size: 11 }
            },
            grid: { 
                color: 'rgba(71, 85, 105, 0.2)',
                drawBorder: false
            },
            border: { display: false }
        },
        y: {
            ticks: { 
                color: '#94a3b8', 
                font: { family: "'Inter', sans-serif", size: 11 }
            },
            grid: { 
                color: 'rgba(71, 85, 105, 0.2)',
                drawBorder: false
            },
            border: { display: false }
        }
    }
};

const BarChart = () => {
    const chartRef = useRef(null);
    const chartInstanceRef = useRef(null);
    const [chartData, setChartData] = useState(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const loadChartData = async () => {
            setLoading(true);
            try {
                const data = await fetchMonthlyChartData();
                if (data && data.length > 0) {
                    // Extract labels and data
                    // Check if data spans multiple years - if so, include year in labels
                    const uniqueYears = new Set(data.map(item => item.year));
                    const showYear = uniqueYears.size > 1;
                    
                    const labels = data.map(item => {
                        if (showYear) {
                            // Show "Apr 2025" format when multiple years
                            return item.month;
                        } else {
                            // Show just "Apr" when single year
                            const monthParts = item.month.split(' ');
                            return monthParts[0];
                        }
                    });
                    const totalTransactions = data.map(item => item.total_transactions || 0);
                    const fraudAlerts = data.map(item => item.fraud_alerts || 0);
                    
                    setChartData({
                        labels,
                        totalTransactions,
                        fraudAlerts
                    });
                } else {
                    // Default empty data
                    setChartData({
                        labels: [],
                        totalTransactions: [],
                        fraudAlerts: []
                    });
                }
            } catch (error) {
                console.error('Error loading chart data:', error);
                setChartData({
                    labels: [],
                    totalTransactions: [],
                    fraudAlerts: []
                });
            } finally {
                setLoading(false);
            }
        };

        loadChartData();
    }, []);

    useEffect(() => {
        if (!chartRef.current || !chartData || loading) return;

        const ctx = chartRef.current.getContext('2d');
        
        // Destroy existing chart if it exists
        if (chartInstanceRef.current) {
            chartInstanceRef.current.destroy();
        }
        
        const gradient1 = ctx.createLinearGradient(0, 0, 0, 400);
        gradient1.addColorStop(0, 'rgba(20, 184, 166, 0.8)');
        gradient1.addColorStop(1, 'rgba(20, 184, 166, 0.2)');
        
        const gradient2 = ctx.createLinearGradient(0, 0, 0, 400);
        gradient2.addColorStop(0, 'rgba(239, 68, 68, 0.8)');
        gradient2.addColorStop(1, 'rgba(239, 68, 68, 0.2)');
        
        const datasets = [
            {
                label: 'Total Transactions',
                data: chartData.totalTransactions,
                backgroundColor: gradient1,
                borderColor: 'rgba(20, 184, 166, 1)',
                borderWidth: 2,
                borderRadius: 8,
                hoverBackgroundColor: 'rgba(20, 184, 166, 1)',
                hoverBorderColor: 'rgba(45, 212, 191, 1)',
                hoverBorderWidth: 3
            }
        ];

        // Always show fraud alerts dataset, even if all zeros (ready for when fraud detection is implemented)
        datasets.push({
            label: 'Fraud Alerts',
            data: chartData.fraudAlerts,
            backgroundColor: gradient2,
            borderColor: 'rgba(239, 68, 68, 1)',
            borderWidth: 2,
            borderRadius: 8,
            hoverBackgroundColor: 'rgba(239, 68, 68, 1)',
            hoverBorderColor: 'rgba(248, 113, 113, 1)',
            hoverBorderWidth: 3
        });
        
        const chart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: chartData.labels,
                datasets: datasets
            },
            options: {
                ...defaultChartOptions,
                interaction: {
                    mode: 'index',
                    intersect: false
                }
            }
        });

        chartInstanceRef.current = chart;

        return () => {
            if (chartInstanceRef.current) {
                chartInstanceRef.current.destroy();
                chartInstanceRef.current = null;
            }
        };
    }, [chartData, loading]);

    if (loading) {
        return (
            <div className="h-80 flex items-center justify-center">
                <div className="text-gray-400">Loading chart data...</div>
            </div>
        );
    }

    if (!chartData || !chartData.labels || chartData.labels.length === 0) {
        return (
            <div className="h-80 flex items-center justify-center">
                <div className="text-gray-400">No chart data available</div>
            </div>
        );
    }

    return <div className="h-80"><canvas ref={chartRef}></canvas></div>;
};

const CategoryChart = () => {
    const chartRef = useRef(null);
    const chartInstanceRef = useRef(null);
    const [chartData, setChartData] = useState(null);
    const [loading, setLoading] = useState(true);

    // Color mapping for different fraud categories
    const getCategoryColor = (category, index) => {
        const colorMap = {
            'Holiday': 'rgba(239, 68, 68, 0.8)', // Red
            'Weekend': 'rgba(236, 72, 153, 0.8)', // Pink
            'Possible Cloning': 'rgba(168, 85, 247, 0.8)' // Purple
        };
        return colorMap[category] || `rgba(${100 + index * 50}, ${150 + index * 30}, ${200 + index * 20}, 0.8)`;
    };

    useEffect(() => {
        const loadChartData = async () => {
            setLoading(true);
            try {
                const data = await fetchCategoryChartData();
                if (data && data.length > 0) {
                    const labels = data.map(item => item.category);
                    const counts = data.map(item => item.count);
                    const colors = labels.map((label, index) => getCategoryColor(label, index));
                    
                    setChartData({
                        labels,
                        counts,
                        colors
                    });
                } else {
                    setChartData({
                        labels: [],
                        counts: [],
                        colors: []
                    });
                }
            } catch (error) {
                console.error('Error loading category chart data:', error);
                setChartData({
                    labels: [],
                    counts: [],
                    colors: []
                });
            } finally {
                setLoading(false);
            }
        };

        loadChartData();
    }, []);

    useEffect(() => {
        if (!chartRef.current || !chartData || loading) return;

        // Destroy existing chart if it exists
        if (chartInstanceRef.current) {
            chartInstanceRef.current.destroy();
        }

        // Don't render chart if no data
        if (chartData.labels.length === 0) {
            return;
        }

        const chart = new Chart(chartRef.current, {
            type: 'doughnut',
            data: {
                labels: chartData.labels,
                datasets: [{
                    label: ' Incidents',
                    data: chartData.counts,
                    backgroundColor: chartData.colors,
                    borderColor: 'rgba(15, 23, 42, 0.8)',
                    borderWidth: 3,
                    hoverOffset: 20,
                    hoverBorderColor: '#fff',
                    hoverBorderWidth: 3
                }]
            },
            options: {
                ...defaultChartOptions,
                cutout: '65%',
                scales: { x: { display: false }, y: { display: false } },
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: { 
                            color: '#e5e7eb', 
                            font: { family: "'Inter', sans-serif", size: 12, weight: '500' },
                            padding: 20,
                            usePointStyle: true,
                            pointStyle: 'circle'
                        }
                    },
                    tooltip: {
                        backgroundColor: 'rgba(15, 23, 42, 0.95)',
                        titleColor: '#f1f5f9',
                        bodyColor: '#cbd5e1',
                        borderColor: '#334155',
                        borderWidth: 1,
                        padding: 12,
                        callbacks: {
                            label: function(context) {
                                let label = context.label || '';
                                let value = context.parsed || 0;
                                let total = context.dataset.data.reduce((a, b) => a + b, 0);
                                let percentage = total > 0 ? ((value / total) * 100).toFixed(1) : 0;
                                return `${label}: ${value} (${percentage}%)`;
                            }
                        }
                    }
                }
            }
        });

        chartInstanceRef.current = chart;

        return () => {
            if (chartInstanceRef.current) {
                chartInstanceRef.current.destroy();
                chartInstanceRef.current = null;
            }
        };
    }, [chartData, loading]);

    if (loading) {
        return (
            <div className="h-80 w-full flex justify-center items-center">
                <div className="text-gray-400">Loading chart data...</div>
            </div>
        );
    }

    if (!chartData || chartData.labels.length === 0) {
        return (
            <div className="h-80 w-full flex justify-center items-center">
                <div className="text-gray-400">No fraud data available</div>
            </div>
        );
    }

    return <div className="h-80 w-full flex justify-center items-center"><canvas ref={chartRef}></canvas></div>;
};

const SeverityChart = () => {
    const chartRef = useRef(null);
    useEffect(() => {
        const chart = new Chart(chartRef.current, {
            type: 'doughnut',
            data: {
                labels: ['High', 'Medium', 'Low'],
                datasets: [{
                    label: ' Severity',
                    data: [25, 35, 40],
                    backgroundColor: [
                        'rgba(239, 68, 68, 0.8)',
                        'rgba(251, 146, 60, 0.8)',
                        'rgba(34, 197, 94, 0.8)'
                    ],
                    borderColor: 'rgba(15, 23, 42, 0.8)',
                    borderWidth: 3,
                    hoverOffset: 20,
                    hoverBorderColor: '#fff',
                    hoverBorderWidth: 3
                }]
            },
            options: {
                ...defaultChartOptions,
                cutout: '65%',
                scales: { x: { display: false }, y: { display: false } },
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: { 
                            color: '#e5e7eb', 
                            font: { family: "'Inter', sans-serif", size: 12, weight: '500' },
                            padding: 20,
                            usePointStyle: true,
                            pointStyle: 'circle'
                        }
                    },
                    tooltip: {
                        backgroundColor: 'rgba(15, 23, 42, 0.95)',
                        titleColor: '#f1f5f9',
                        bodyColor: '#cbd5e1',
                        borderColor: '#334155',
                        borderWidth: 1,
                        padding: 12,
                        callbacks: {
                            label: function(context) {
                                let label = context.label || '';
                                let value = context.parsed || 0;
                                let total = context.dataset.data.reduce((a, b) => a + b, 0);
                                let percentage = ((value / total) * 100).toFixed(1);
                                return `${label}: ${value} (${percentage}%)`;
                            }
                        }
                    }
                }
            }
        });
        return () => chart.destroy();
    }, []);
    return <div className="h-80 w-full flex justify-center items-center"><canvas ref={chartRef}></canvas></div>;
};

// --- Risk Gauge Component ---

/*
const RiskGauge = ({ score, label }) => {
    const getColor = (score) => {
        if (score >= 80) return { bg: 'from-red-500 to-rose-600', text: 'text-red-400', ring: 'ring-red-500/20' };
        if (score >= 50) return { bg: 'from-orange-500 to-amber-600', text: 'text-orange-400', ring: 'ring-orange-500/20' };
        return { bg: 'from-emerald-500 to-green-600', text: 'text-emerald-400', ring: 'ring-emerald-500/20' };
    };
    
    const colors = getColor(score);
    const circumference = 2 * Math.PI * 45;
    const offset = circumference - (score / 100) * circumference;
    
    return (
        <div className="flex flex-col items-center">
            <div className="relative w-28 h-28">
                <svg className="transform -rotate-90 w-28 h-28">
                    <circle
                        cx="56"
                        cy="56"
                        r="45"
                        stroke="currentColor"
                        strokeWidth="8"
                        fill="none"
                        className="text-slate-700"
                    />
                    <circle
                        cx="56"
                        cy="56"
                        r="45"
                        stroke="url(#gradient)"
                        strokeWidth="8"
                        fill="none"
                        strokeDasharray={circumference}
                        strokeDashoffset={offset}
                        strokeLinecap="round"
                        className="transition-all duration-1000 ease-out"
                    />
                    <defs>
                        <linearGradient id="gradient" x1="0%" y1="0%" x2="100%" y2="100%">
                            <stop offset="0%" className={colors.bg.split(' ')[0].replace('from-', 'stop-')} />
                            <stop offset="100%" className={colors.bg.split(' ')[1].replace('to-', 'stop-')} />
                        </linearGradient>
                    </defs>
                </svg>
                <div className="absolute inset-0 flex items-center justify-center">
                    <span className={`text-2xl font-bold ${colors.text}`}>{score}</span>
                </div>
            </div>
            <span className="mt-2 text-sm text-gray-400 font-medium">{label}</span>
        </div>
    );
};
*/
// --- View Components ---

const DashboardView = ({ setActiveView }) => {
    const [animate, setAnimate] = useState(false);
    const [metrics, setMetrics] = useState({
        total_alerts_ytd: 0,
        detected_frauds_current_month: 0,
        total_amount: 0
    });
    const [recentFlaggedTransactions, setRecentFlaggedTransactions] = useState([]);
    const [loading, setLoading] = useState(true);
    
    useEffect(() => {
        setAnimate(true);
        const loadDashboardData = async () => {
            setLoading(true);
            try {
                const [metricsData, alertsData] = await Promise.all([
                    fetchMetrics(),
                    fetchRecentAlerts()
                ]);
                setMetrics(metricsData);
                // Get top 3 most recent flagged transactions
                const recent = alertsData
                    .filter(txn => txn.flag_fraud === true || txn.status === 'Flagged' || txn.status === 'Investigating')
                    .slice(0, 3);
                setRecentFlaggedTransactions(recent);
            } catch (error) {
                console.error('Error loading dashboard data:', error);
            } finally {
                setLoading(false);
            }
        };
        loadDashboardData();
    }, []);

    return (
        <div className="space-y-6">
            {/* Stats Cards */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                {/* Total Alerts Card */}
                <div className={`group bg-gradient-to-br from-slate-800/80 to-slate-800/40 backdrop-blur-sm border border-slate-700/50 p-6 rounded-2xl shadow-xl hover:shadow-2xl hover:border-teal-500/50 transition-all duration-300 transform hover:-translate-y-1 ${animate ? 'animate-fadeIn' : 'opacity-0'}`}>
                    <div className="flex items-center mb-4">
                        <div className="p-3 bg-gradient-to-br from-teal-500/20 to-teal-600/10 rounded-xl ring-2 ring-teal-500/20 group-hover:ring-teal-500/40 transition-all">
                            <ShieldIcon />
                        </div>
                    </div>
                    <h3 className="text-gray-400 text-sm font-medium mb-1">Total Alerts (YTD)</h3>
                    <p className="text-4xl font-bold text-white mb-2 bg-gradient-to-r from-teal-400 to-teal-200 bg-clip-text text-transparent">
                        {loading ? '...' : metrics.total_alerts_ytd.toLocaleString()}
                    </p>
                </div>

                {/* Potential Loss Card */}
                <div className={`group bg-gradient-to-br from-slate-800/80 to-slate-800/40 backdrop-blur-sm border border-slate-700/50 p-6 rounded-2xl shadow-xl hover:shadow-2xl hover:border-rose-500/50 transition-all duration-300 transform hover:-translate-y-1 ${animate ? 'animate-fadeIn' : 'opacity-0'}`} style={{animationDelay: '0.1s'}}>
                    <div className="flex items-center mb-4">
                        <div className="p-3 bg-gradient-to-br from-rose-500/20 to-rose-600/10 rounded-xl ring-2 ring-rose-500/20 group-hover:ring-rose-500/40 transition-all">
                            <DollarIcon />
                        </div>
                    </div>
                    <h3 className="text-gray-400 text-sm font-medium mb-1">Potential Loss (YTD)</h3>
                    <p className="text-4xl font-bold text-white mb-2 bg-gradient-to-r from-rose-400 to-rose-200 bg-clip-text text-transparent">
                        {loading ? '...' : `$${Math.abs(metrics.total_amount || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`}
                    </p>
                </div>

                {/* Detected Frauds Card */}
                <div className={`group bg-gradient-to-br from-slate-800/80 to-slate-800/40 backdrop-blur-sm border border-slate-700/50 p-6 rounded-2xl shadow-xl hover:shadow-2xl hover:border-amber-500/50 transition-all duration-300 transform hover:-translate-y-1 ${animate ? 'animate-fadeIn' : 'opacity-0'}`} style={{animationDelay: '0.2s'}}>
                    <div className="flex items-center mb-4">
                        <div className="p-3 bg-gradient-to-br from-amber-500/20 to-amber-600/10 rounded-xl ring-2 ring-amber-500/20 group-hover:ring-amber-500/40 transition-all">
                            <AlertIcon />
                        </div>
                    </div>
                    <h3 className="text-gray-400 text-sm font-medium mb-1">Detected Frauds (Current Month)</h3>
                    <p className="text-4xl font-bold text-white mb-2 bg-gradient-to-r from-amber-400 to-amber-200 bg-clip-text text-transparent">
                        {loading ? '...' : metrics.detected_frauds_current_month.toLocaleString()}
                    </p>
                </div>
            </div>

            {/* Charts Grid */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                {/* Bar Chart */}
                <div className="lg:col-span-2 bg-gradient-to-br from-slate-800/80 to-slate-800/40 backdrop-blur-sm border border-slate-700/50 p-6 rounded-2xl shadow-xl hover:shadow-2xl transition-all duration-300">
                    <div className="flex items-center justify-between mb-6">
                        <div>
                            <h3 className="font-bold text-xl text-white mb-1">Transaction Analysis</h3>
                            <p className="text-sm text-gray-400">Month-over-month comparison</p>
                        </div>
                        <div className="flex space-x-2">
                            <button className="px-3 py-1 text-xs font-medium bg-teal-500/20 text-teal-400 rounded-lg hover:bg-teal-500/30 transition-colors">6M</button>
                            <button className="px-3 py-1 text-xs font-medium bg-slate-700/50 text-gray-400 rounded-lg hover:bg-slate-700 transition-colors">1Y</button>
                        </div>
                    </div>
                    <BarChart />
                </div>

                {/* Alert Feed */}
                <div className="bg-gradient-to-br from-slate-800/80 to-slate-800/40 backdrop-blur-sm border border-slate-700/50 p-6 rounded-2xl shadow-xl hover:shadow-2xl transition-all duration-300">
                    <div className="flex items-center justify-between mb-6">
                        <h3 className="font-bold text-xl text-white">Recent Flagged Transactions</h3>
                        <span className="px-2 py-1 bg-amber-500/20 text-amber-400 text-xs font-semibold rounded-full">{recentFlaggedTransactions.length} New</span>
                    </div>
                    <div className="space-y-4">
                        {loading ? (
                            <div className="text-gray-400 text-center py-4">Loading alerts...</div>
                        ) : recentFlaggedTransactions.length === 0 ? (
                            <div className="text-gray-400 text-center py-4">No recent flagged transactions</div>
                        ) : (
                            recentFlaggedTransactions.map((txn, idx) => {
                                const status = txn.status || (txn.flag_fraud ? 'Flagged' : 'Resolved');
                                const isInvestigating = status === 'Investigating';
                                const category = txn.vehicle_type_name || txn.triggered_flags || 'Fraud Alert';
                                
                                return (
                                    <div key={txn.transaction_id || idx} className={`p-4 rounded-xl border transition-all duration-300 hover:scale-105 cursor-pointer ${
                                        isInvestigating 
                                            ? 'bg-red-500/10 border-red-500/30 hover:border-red-500/50' 
                                            : 'bg-amber-500/10 border-amber-500/30 hover:border-amber-500/50'
                                    }`}>
                                        <div className="flex items-start justify-between">
                                            <div className="flex-1">
                                                <div className="flex items-center gap-2 mb-2">
                                                    <span className="font-mono text-xs text-teal-400 font-semibold">{txn.tag_plate_number || 'N/A'}</span>
                                                    <span className={`px-2 py-0.5 rounded text-xs font-semibold ${
                                                        isInvestigating ? 'bg-red-500/20 text-red-400' : 'bg-amber-500/20 text-amber-400'
                                                    }`}>
                                                        {status}
                                                    </span>
                                                </div>
                                                <p className="text-sm text-white font-medium mb-1">{category}</p>
                                                <div className="flex items-center gap-3 text-xs text-gray-400">
                                                    <span>${(txn.amount || 0).toFixed(2)}</span>
                                                    <span>•</span>
                                                    <span>{txn.agency || txn.agency_name || '-'}</span>
                                                </div>
                                            </div>
                                            <div className={`w-2 h-2 rounded-full mt-1 ${
                                                isInvestigating ? 'bg-red-500 animate-pulse' : 'bg-amber-500 animate-pulse'
                                            }`}></div>
                                        </div>
                                    </div>
                                );
                            })
                        )}
                        <button 
                            onClick={() => setActiveView('data')}
                            className="w-full py-3 text-sm font-medium text-teal-400 hover:text-teal-300 transition-colors border border-slate-700 hover:border-teal-500/50 rounded-xl"
                        >
                            View All Alerts →
                        </button>
                    </div>
                </div>
            </div>

            {/* Bottom Charts */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Category Chart */}
                <div className="bg-gradient-to-br from-slate-800/80 to-slate-800/40 backdrop-blur-sm border border-slate-700/50 p-6 rounded-2xl shadow-xl hover:shadow-2xl transition-all duration-300">
                    <div className="mb-6">
                        <h3 className="font-bold text-xl text-white mb-1">Fraud by Category</h3>
                        <p className="text-sm text-gray-400">Distribution across incident types</p>
                    </div>
                    <CategoryChart />
                </div>

                {/* Severity Chart */}
                <div className="bg-gradient-to-br from-slate-800/80 to-slate-800/40 backdrop-blur-sm border border-slate-700/50 p-6 rounded-2xl shadow-xl hover:shadow-2xl transition-all duration-300">
                    <div className="mb-6">
                        <h3 className="font-bold text-xl text-white mb-1">Threat Severity</h3>
                        <p className="text-sm text-gray-400">Risk level distribution</p>
                    </div>
                    <SeverityChart />
                </div>
            </div>
        </div>
    );
};

const DataView = () => {
    const [searchTerm, setSearchTerm] = useState('');
    const [filterStatus, setFilterStatus] = useState('all');
    const [transactionData, setTransactionData] = useState([]);
    const [loading, setLoading] = useState(true);
    const [currentPage, setCurrentPage] = useState(1);
    const itemsPerPage = 15; // rows per page

    useEffect(() => {
        const loadData = async () => {
            setLoading(true);
            const data = await fetchTransactions();
            setTransactionData(data);
            setLoading(false);
        };
        loadData();
    }, []);

    // Reset to page 1 when search or filter changes
    useEffect(() => {
        setCurrentPage(1);
    }, [searchTerm, filterStatus]);

    const handleStatusChange = (transactionId, newStatus) => {
        setTransactionData(prevData => 
            prevData.map(transaction => 
                (transaction.transaction_id || transaction.id) === transactionId 
                    ? { ...transaction, status: newStatus }
                    : transaction
            )
        );
    };

    // Helper function to handle null/undefined values
    const formatValue = (value) => {
        if (value === null || value === undefined || value === '') {
            return '-';
        }
        return value;
    };

    // Helper function to format dates (MM/DD/YYYY)
    const formatDate = (dateValue) => {
        if (dateValue === null || dateValue === undefined || dateValue === '' || dateValue === '-') {
            return '-';
        }
        
        try {
            const date = new Date(dateValue);
            if (isNaN(date.getTime())) {
                return '-'; // Return '-' if invalid date
            }
            
            // Format as MM/DD/YYYY
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const day = String(date.getDate()).padStart(2, '0');
            const year = date.getFullYear();
            return `${month}/${day}/${year}`;
        } catch (e) {
            return '-'; // Return '-' if parsing fails
        }
    };

    // Helper function to format date with time (MM/DD/YYYY HH:MM AM/PM)
    const formatDateTime = (dateValue) => {
        if (dateValue === null || dateValue === undefined || dateValue === '' || dateValue === '-') {
            return '-';
        }
        
        try {
            const date = new Date(dateValue);
            if (isNaN(date.getTime())) {
                return '-'; // Return '-' if invalid date
            }
            
            // Format as MM/DD/YYYY HH:MM AM/PM
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const day = String(date.getDate()).padStart(2, '0');
            const year = date.getFullYear();
            
            // Convert to 12-hour format with AM/PM
            let hours = date.getHours();
            const minutes = String(date.getMinutes()).padStart(2, '0');
            const ampm = hours >= 12 ? 'PM' : 'AM';
            hours = hours % 12;
            hours = hours ? hours : 12; // the hour '0' should be '12'
            
            return `${month}/${day}/${year} ${hours}:${minutes} ${ampm}`;
        } catch (e) {
            return '-'; // Return '-' if parsing fails
        }
    };

    const filteredData = transactionData.filter(row => {
        const id = row.transaction_id || row.id || '';
        const category = row.vehicle_type_name || row.triggered_flags || row.category || '';
        const tagPlate = row.tag_plate_number || '';
        const matchesSearch = id.toLowerCase().includes(searchTerm.toLowerCase()) ||
                            category.toLowerCase().includes(searchTerm.toLowerCase()) ||
                            tagPlate.toLowerCase().includes(searchTerm.toLowerCase());
        // Derive status from flag_fraud if status field doesn't exist
        const rowStatus = row.status || (row.flag_fraud ? 'Flagged' : 'Resolved');
        const matchesFilter = filterStatus === 'all' || rowStatus === filterStatus;
        return matchesSearch && matchesFilter;
    });

    // Slice the filtered data for the current page
    const paginatedData = filteredData.slice(
        (currentPage - 1) * itemsPerPage,
        currentPage * itemsPerPage
    );

    // Total pages
    const totalPages = Math.ceil(filteredData.length / itemsPerPage);



    if (loading) return <div>Loading transactions...</div>;
    return (
        <div className="space-y-6">
            {/* Search and Filter Bar */}
            <div className="bg-gradient-to-br from-slate-800/80 to-slate-800/40 backdrop-blur-sm border border-slate-700/50 p-6 rounded-2xl shadow-xl">
                <div className="flex flex-col md:flex-row gap-4">
                    <div className="flex-1 relative">
                        <input
                            type="text"
                            placeholder="Search transactions..."
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                            className="w-full bg-slate-900/50 border border-slate-700 rounded-xl px-4 py-3 pl-11 text-white placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-indigo-500/50 focus:border-indigo-500/50 transition-all"
                        />
                        <div className="absolute left-3 top-3.5 text-gray-500">
                            <SearchIcon />
                        </div>
                    </div>
                    <div className="flex space-x-2">
                        <button
                            onClick={() => setFilterStatus('all')}
                            className={`px-4 py-3 rounded-xl text-sm font-medium transition-all ${
                                filterStatus === 'all'
                                    ? 'bg-teal-600 text-white'
                                    : 'bg-slate-700/50 text-gray-400 hover:bg-slate-700'
                            }`}
                        >
                            All
                        </button>
                        <button
                            onClick={() => setFilterStatus('Flagged')}
                            className={`px-4 py-3 rounded-xl text-sm font-medium transition-all ${
                                filterStatus === 'Flagged'
                                    ? 'bg-amber-500 text-white'
                                    : 'bg-slate-700/50 text-gray-400 hover:bg-slate-700'
                            }`}
                        >
                            Flagged
                        </button>
                        <button
                            onClick={() => setFilterStatus('Investigating')}
                            className={`px-4 py-3 rounded-xl text-sm font-medium transition-all ${
                                filterStatus === 'Investigating'
                                    ? 'bg-red-500 text-white'
                                    : 'bg-slate-700/50 text-gray-400 hover:bg-slate-700'
                            }`}
                        >
                            Investigating
                        </button>
                        <button
                            onClick={() => setFilterStatus('Resolved')}
                            className={`px-4 py-3 rounded-xl text-sm font-medium transition-all ${
                                filterStatus === 'Resolved'
                                    ? 'bg-emerald-500 text-white'
                                    : 'bg-slate-700/50 text-gray-400 hover:bg-slate-700'
                            }`}
                        >
                            Resolved
                        </button>
                    </div>
                </div>
            </div>

            {/* Data Table */}
            <div className="bg-gradient-to-br from-slate-800/80 to-slate-800/40 backdrop-blur-sm border border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
                <div className="overflow-x-auto">
                    <table className="w-full text-base table-fixed">
                        <thead className="bg-slate-900/50 border-b border-slate-700">
                            <tr>
                                <th scope="col" className="px-4 py-4 text-left text-sm font-semibold text-gray-400 uppercase tracking-wider w-[11%]">Tag/Plate Number</th>
                                <th scope="col" className="px-4 py-4 text-left text-sm font-semibold text-gray-400 uppercase tracking-wider w-[9%]">Transaction Date</th>
                                <th scope="col" className="px-4 py-4 text-left text-sm font-semibold text-gray-400 uppercase tracking-wider w-[7%]">Agency</th>
                                <th scope="col" className="px-4 py-4 text-left text-sm font-semibold text-gray-400 uppercase tracking-wider w-[12%]">Entry Time</th>
                                <th scope="col" className="px-4 py-4 text-left text-sm font-semibold text-gray-400 uppercase tracking-wider w-[6%]">Entry Plaza</th>
                                <th scope="col" className="px-4 py-4 text-left text-sm font-semibold text-gray-400 uppercase tracking-wider w-[12%]">Exit Time</th>
                                <th scope="col" className="px-4 py-4 text-left text-sm font-semibold text-gray-400 uppercase tracking-wider w-[6%]">Exit Plaza</th>
                                <th scope="col" className="px-4 py-4 text-left text-sm font-semibold text-gray-400 uppercase tracking-wider w-[9%]">Amount</th>
                                <th scope="col" className="px-4 py-4 text-left text-sm font-semibold text-gray-400 uppercase tracking-wider w-[11%]">Status</th>
                                <th scope="col" className="px-4 py-4 text-left text-sm font-semibold text-gray-400 uppercase tracking-wider w-[17%]">Category</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-700/50">
                            {paginatedData.map((row, index) => {
                                const tagPlate = formatValue(row.tag_plate_number);
                                const transactionDate = formatDate(row.transaction_date);
                                const agency = formatValue(row.agency || row.agency_name);
                                const entryTime = formatDateTime(row.entry_time);
                                const entryPlaza = formatValue(row.entry_plaza || row.entry_plaza_name);
                                const exitTime = formatDateTime(row.exit_time);
                                const exitPlaza = formatValue(row.exit_plaza || row.exit_plaza_name);
                                const amount = row.amount !== null && row.amount !== undefined ? `$${parseFloat(row.amount).toFixed(2)}` : '-';
                                // Derive status from flag_fraud if status field doesn't exist
                                const status = formatValue(row.status || (row.flag_fraud ? 'Flagged' : 'Resolved'));
                                const category = formatValue(row.vehicle_type_name || row.triggered_flags || row.category);

                                return (
                                    <tr 
                                        key={row.transaction_id || row.id || index} 
                                        className="hover:bg-slate-700/30 transition-colors duration-150 group"
                                    >
                                        <td className="px-4 py-4 whitespace-nowrap">
                                            <span className="font-mono text-gray-100 font-medium text-base">{tagPlate}</span>
                                        </td>
                                        <td className="px-4 py-4 whitespace-nowrap text-gray-300 text-base">
                                            {transactionDate}
                                        </td>
                                        <td className="px-4 py-4 whitespace-nowrap">
                                            {agency !== '-' ? (
                                                <span className={`inline-block px-2 py-1 rounded-lg text-xs font-semibold ${
                                                    agency === 'PANYNJ' ? 'bg-blue-500/20 text-blue-400 ring-1 ring-blue-500/30' :
                                                    'bg-purple-500/20 text-purple-400 ring-1 ring-purple-500/30'
                                                }`}>
                                                    {agency}
                                                </span>
                                            ) : (
                                                <span className="text-gray-400 text-base">-</span>
                                            )}
                                        </td>
                                        <td className="px-4 py-4 whitespace-nowrap text-gray-300 text-base">
                                            {entryTime}
                                        </td>
                                        <td className="px-4 py-4 whitespace-nowrap text-gray-300 text-base">
                                            {entryPlaza}
                                        </td>
                                        <td className="px-4 py-4 whitespace-nowrap text-gray-300 text-base">
                                            {exitTime}
                                        </td>
                                        <td className="px-4 py-4 whitespace-nowrap text-gray-300 text-base">
                                            {exitPlaza}
                                        </td>
                                        <td className="px-4 py-4 whitespace-nowrap">
                                            <span className="font-semibold text-white text-base">{amount}</span>
                                        </td>
                                        <td className="px-4 py-4 whitespace-nowrap">
                                            {status !== '-' ? (
                                                <select
                                                    value={status}
                                                    onChange={(e) => handleStatusChange(row.transaction_id || row.id, e.target.value)}
                                                    className={`px-2 py-1.5 rounded-lg text-xs font-semibold border-0 cursor-pointer focus:outline-none focus:ring-2 focus:ring-offset-0 ${
                                                        status === 'Investigating' ? 'bg-red-500/20 text-red-400 focus:ring-red-500/50' :
                                                        status === 'Flagged' ? 'bg-amber-500/20 text-amber-400 focus:ring-amber-500/50' :
                                                        'bg-emerald-500/20 text-emerald-400 focus:ring-emerald-500/50'
                                                    }`}
                                                >
                                                    <option value="Flagged" className="bg-slate-800 text-amber-400">Flagged</option>
                                                    <option value="Investigating" className="bg-slate-800 text-red-400">Investigating</option>
                                                    <option value="Resolved" className="bg-slate-800 text-emerald-400">Resolved</option>
                                                </select>
                                            ) : (
                                                <span className="text-gray-400 text-base">-</span>
                                            )}
                                        </td>
                                        <td className="px-4 py-4 whitespace-nowrap">
                                            {category !== '-' ? (
                                                <span className={`inline-block px-2 py-1 rounded-lg text-xs font-semibold ${
                                                    category === 'Account Takeover' ? 'bg-pink-500/20 text-pink-400 ring-1 ring-pink-500/30' :
                                                    category === 'Card Skimming' ? 'bg-purple-500/20 text-purple-400 ring-1 ring-purple-500/30' :
                                                    'bg-cyan-500/20 text-cyan-400 ring-1 ring-cyan-500/30'
                                                }`}>
                                                    {category}
                                                </span>
                                            ) : (
                                                <span className="text-gray-400 text-base">-</span>
                                            )}
                                        </td>
                                    </tr>
                                );
                            })}
                        </tbody>
                    </table>
                </div>
                
                {/* Pagination */}
                <div className="bg-slate-900/50 px-6 py-4 border-t border-slate-700 flex flex-col sm:flex-row items-center justify-between gap-4">
                    <div className="text-sm text-gray-400">
                        Showing <span className="font-semibold text-white">
                            {paginatedData.length > 0 ? (currentPage - 1) * itemsPerPage + 1 : 0}
                        </span> to <span className="font-semibold text-white">
                            {Math.min(currentPage * itemsPerPage, filteredData.length)}
                        </span> of <span className="font-semibold text-white">{filteredData.length}</span> transactions
                    </div>
                    <div className="flex items-center space-x-2">
                        <button 
                            onClick={() => setCurrentPage(p => Math.max(p - 1, 1))}
                            disabled={currentPage === 1 || totalPages === 0}
                            className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                                currentPage === 1 || totalPages === 0
                                    ? 'bg-slate-700/30 text-gray-500 cursor-not-allowed' 
                                    : 'bg-slate-700/50 text-gray-300 hover:bg-slate-700 hover:text-white'
                            }`}
                        >
                            Previous
                        </button>

                        {totalPages > 0 && (
                            <div className="flex space-x-1">
                                {[...Array(Math.min(totalPages, 10))].map((_, idx) => {
                                    // Show page numbers with ellipsis for large page counts
                                    let pageNum;
                                    if (totalPages <= 10) {
                                        pageNum = idx + 1;
                                    } else if (currentPage <= 5) {
                                        pageNum = idx + 1;
                                    } else if (currentPage >= totalPages - 4) {
                                        pageNum = totalPages - 9 + idx;
                                    } else {
                                        pageNum = currentPage - 5 + idx;
                                    }

                                    return (
                                        <button
                                            key={idx}
                                            onClick={() => setCurrentPage(pageNum)}
                                            className={`px-3 py-2 rounded-lg text-sm font-medium transition-all ${
                                                currentPage === pageNum 
                                                    ? 'bg-teal-600 text-white ring-2 ring-teal-500/50' 
                                                    : 'bg-slate-700/50 text-gray-400 hover:bg-slate-700 hover:text-white'
                                            }`}
                                        >
                                            {pageNum}
                                        </button>
                                    );
                                })}
                            </div>
                        )}

                        <button 
                            onClick={() => setCurrentPage(p => Math.min(p + 1, totalPages))}
                            disabled={currentPage === totalPages || totalPages === 0}
                            className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                                currentPage === totalPages || totalPages === 0
                                    ? 'bg-slate-700/30 text-gray-500 cursor-not-allowed' 
                                    : 'bg-slate-700/50 text-gray-300 hover:bg-slate-700 hover:text-white'
                            }`}
                        >
                            Next
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
};

// --- Main App Component ---

export default function App() {
    const [activeView, setActiveView] = useState('dashboard');

    return (
        <div className="relative bg-slate-950 text-gray-200 min-h-screen" style={{ fontFamily: "'Inter', sans-serif" }}>
            {/* Animated Background */}
            <div className="fixed inset-0 overflow-hidden pointer-events-none">
                <div className="absolute -top-1/2 -right-1/2 w-full h-full bg-gradient-to-br from-teal-500/10 via-cyan-500/5 to-transparent rounded-full blur-3xl animate-pulse"></div>
                <div className="absolute -bottom-1/2 -left-1/2 w-full h-full bg-gradient-to-tr from-blue-500/10 via-teal-500/5 to-transparent rounded-full blur-3xl animate-pulse" style={{animationDelay: '1s'}}></div>
            </div>

            <div className="relative z-10 p-4 sm:p-6 lg:p-8">
                <div className="max-w-7xl mx-auto">
                    {/* Header */}
                    <header className="mb-8">
                        <div className="bg-gradient-to-r from-slate-800/50 via-slate-800/30 to-slate-800/50 backdrop-blur-xl border border-slate-700/50 rounded-2xl p-6 shadow-2xl">
                            <div className="flex flex-col lg:flex-row justify-between items-start lg:items-center gap-6">
                                {/* Logo and Title */}
                                <div className="flex items-center space-x-4">
                                    <div className="bg-gradient-to-br from-teal-600 to-teal-700 p-3 rounded-2xl shadow-lg">
                                        <ShieldIcon />
                                    </div>
                                    <div>
                                        <h1 className="text-3xl md:text-4xl font-bold bg-gradient-to-r from-white via-gray-100 to-gray-300 bg-clip-text text-transparent">
                                            EZ Pass Fraud Detection
                                        </h1>
                                        <p className="text-sm text-gray-400 mt-1">New Jersey Courts - Advanced Security System</p>
                                    </div>
                                </div>

                                {/* Navigation Toggle */}
                                <div className="flex items-center p-1.5 rounded-xl bg-slate-900/50 backdrop-blur-sm border border-slate-700/50 shadow-inner">
                                    <button 
                                        onClick={() => setActiveView('dashboard')} 
                                        className={`px-6 py-2.5 text-sm font-semibold rounded-lg focus:outline-none transition-all duration-300 ${
                                            activeView === 'dashboard' 
                                                ? 'bg-gradient-to-r from-teal-600 to-teal-700 text-white' 
                                                : 'text-gray-400 hover:text-white'
                                        }`}
                                    >
                                        📊 Dashboard
                                    </button>
                                    <button 
                                        onClick={() => setActiveView('data')} 
                                        className={`px-6 py-2.5 text-sm font-semibold rounded-lg focus:outline-none transition-all duration-300 ${
                                            activeView === 'data' 
                                                ? 'bg-gradient-to-r from-teal-600 to-teal-700 text-white' 
                                                : 'text-gray-400 hover:text-white'
                                        }`}
                                    >
                                        📋 Raw Data
                                    </button>
                                </div>
                            </div>
                        </div>
                    </header>

                    {/* Main Content */}
                    <main className="animate-fadeIn">
                        {activeView === 'dashboard' ? <DashboardView setActiveView={setActiveView} /> : <DataView />}
                    </main>
                </div>
            </div>
        </div>
    );
}