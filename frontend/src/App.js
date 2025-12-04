import React, { useState, useEffect, useRef } from 'react';
import { Chart, registerables } from 'chart.js';
import Plot from 'react-plotly.js';
import { useMsal, useIsAuthenticated } from "@azure/msal-react";
import { loginRequest } from "./authConfig";

Chart.register(...registerables);

const apiUrl = process.env.REACT_APP_API_URL;

const fetchTransactions = async (page = 1, limit = 50, search = '', status = 'all', category = 'all') => {
    try {
        const params = new URLSearchParams({
            page: page.toString(),
            limit: limit.toString(),
        });
        if (search) params.append('search', search);
        if (status && status !== 'all') params.append('status', status);
        if (category && category !== 'all') params.append('category', category);
        
        const response = await fetch(`${apiUrl}/api/transactions?${params.toString()}`);
        if (!response.ok) throw new Error('Failed to fetch data');
        const result = await response.json();
        return result.data || [];
    } catch (err) {
        console.error(err);
        return [];
    }
};

const fetchTransactionsCount = async (search = '', status = 'all', category = 'all') => {
    try {
        const params = new URLSearchParams();
        if (search) params.append('search', search);
        if (status && status !== 'all') params.append('status', status);
        if (category && category !== 'all') params.append('category', category);
        
        const response = await fetch(`${apiUrl}/api/transactions/count?${params.toString()}`);
        if (!response.ok) throw new Error('Failed to fetch count');
        const result = await response.json();
        return result.total || 0;
    } catch (err) {
        console.error(err);
        return 0;
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

const fetchSeverityChartData = async () => {
    try {
        const response = await fetch(`${apiUrl}/api/charts/severity`);
        if (!response.ok) throw new Error('Failed to fetch severity chart data');
        const { data } = await response.json();
        return data || [];
    } catch (err) {
        console.error(err);
        return [];
    }
};

const fetchTableInfo = async () => {
    try {
        const response = await fetch(`${apiUrl}/api/table-info`);
        if (!response.ok) throw new Error('Failed to fetch table info');
        const data = await response.json();
        return data;
    } catch (err) {
        console.error(err);
        return { table_name: 'master_viz', is_master_viz: true, is_gold_automation: false };
    }
};

const fetchRecentFlaggedTransactions = async () => {
    try {
        const response = await fetch(`${apiUrl}/api/transactions/recent-flagged`);
        if (!response.ok) throw new Error('Failed to fetch recent flagged transactions');
        const { data } = await response.json();
        return data || [];
    } catch (err) {
        console.error(err);
        return [];
    }
};

const fetchDashboardMetrics = async () => {
    try {
        const response = await fetch(`${apiUrl}/api/metrics`);
        if (!response.ok) throw new Error('Failed to fetch dashboard metrics');
        const data = await response.json();
        return data || {};
    } catch (err) {
        console.error(err);
        return {
            total_alerts_ytd: 0,
            total_amount: 0,
            detected_frauds_current_month: 0,
            potential_loss_ytd: 0
        };
    }
};

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

const SortUpIcon = () => (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 15l7-7 7 7" />
    </svg>
);

const SortDownIcon = () => (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
    </svg>
);

const SortIcon = () => (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16V4m0 0L3 8m4-4l4 4m6 0v12m0 0l4-4m-4 4l-4-4" />
    </svg>
);

// --- Chart Components ---

const getChartOptions = (isDarkMode) => ({
    maintainAspectRatio: false,
    responsive: true,
    plugins: {
        legend: {
            labels: {
                color: isDarkMode ? '#e5e7eb' : '#1e293b',
                font: { family: "'Inter', sans-serif", size: 12, weight: '500' },
                padding: 15,
                usePointStyle: true,
                pointStyle: 'circle'
            }
        },
        tooltip: {
            backgroundColor: isDarkMode ? 'rgba(15, 23, 42, 0.95)' : 'rgba(255, 255, 255, 0.95)',
            titleColor: isDarkMode ? '#f1f5f9' : '#1e293b',
            bodyColor: isDarkMode ? '#cbd5e1' : '#475569',
            borderColor: isDarkMode ? '#334155' : '#cbd5e1',
            borderWidth: 1,
            padding: 12,
            displayColors: true,
            boxPadding: 6
        }
    },
    scales: {
        x: {
            ticks: { 
                color: isDarkMode ? '#94a3b8' : '#64748b', 
                font: { family: "'Inter', sans-serif", size: 11 }
            },
            grid: { 
                color: isDarkMode ? 'rgba(71, 85, 105, 0.2)' : 'rgba(148, 163, 184, 0.2)',
                drawBorder: false
            },
            border: { display: false }
        },
        y: {
            ticks: { 
                color: isDarkMode ? '#94a3b8' : '#64748b', 
                font: { family: "'Inter', sans-serif", size: 11 }
            },
            grid: { 
                color: isDarkMode ? 'rgba(71, 85, 105, 0.2)' : 'rgba(148, 163, 184, 0.2)',
                drawBorder: false
            },
            border: { display: false }
        }
    }
});

const BarChart = () => {
    const chartRef = useRef(null);
    const chartInstanceRef = useRef(null);
    const [chartData, setChartData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [isDarkMode, setIsDarkMode] = useState(document.documentElement.classList.contains('dark'));

    useEffect(() => {
        const checkTheme = () => {
            setIsDarkMode(document.documentElement.classList.contains('dark'));
        };
        const observer = new MutationObserver(checkTheme);
        observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });
        checkTheme();
        return () => observer.disconnect();
    }, []);

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
        Chart.getChart(chartRef.current)?.destroy();
        chartInstanceRef.current = null;
        
        const gradient1 = ctx.createLinearGradient(0, 0, 0, 400);
        gradient1.addColorStop(0, 'rgba(149, 70, 167, 0.8)');
        gradient1.addColorStop(1, 'rgba(149, 70, 167, 0.2)');
        
        const gradient2 = ctx.createLinearGradient(0, 0, 0, 400);
        gradient2.addColorStop(0, 'rgba(239, 68, 68, 0.8)');
        gradient2.addColorStop(1, 'rgba(239, 68, 68, 0.2)');
        
        const datasets = [
            {
                label: 'Total Transactions',
                data: chartData.totalTransactions,
                backgroundColor: gradient1,
                borderColor: 'rgba(149, 70, 167, 1)',
                borderWidth: 2,
                borderRadius: 8,
                hoverBackgroundColor: 'rgba(149, 70, 167, 1)',
                hoverBorderColor: 'rgba(149, 70, 167, 1)',
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
                ...getChartOptions(isDarkMode),
                interaction: {
                    mode: 'index',
                    intersect: false
                },
                scales: {
                    x: {
                        ticks: {
                            font: {
                                size: 15   // change to whatever size you want
                            }
                        }
                    },
                    y: {
                        ticks: {
                            font: {
                                size: 15
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
    }, [chartData, loading, isDarkMode]);

    if (loading) {
        return (
            <div className="h-80 flex items-center justify-center">
                <div className="text-gray-400 dark:text-gray-400 text-gray-600">Loading chart data...</div>
            </div>
        );
    }

    if (!chartData || !chartData.labels || chartData.labels.length === 0) {
        return (
            <div className="h-80 flex items-center justify-center">
                <div className="text-gray-400 dark:text-gray-400 text-gray-600">No chart data available</div>
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
    const [isDarkMode, setIsDarkMode] = useState(document.documentElement.classList.contains('dark'));

    useEffect(() => {
        const checkTheme = () => {
            setIsDarkMode(document.documentElement.classList.contains('dark'));
        };
        const observer = new MutationObserver(checkTheme);
        observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });
        checkTheme();
        return () => observer.disconnect();
    }, []);

    // Color mapping for different fraud categories - using vibrant colors visible in light mode
    const getCategoryColor = (category, index) => {
        const colorMap = {
            // Master_viz categories
            'Holiday': 'rgba(239, 68, 68, 0.9)', // Red
            'Out of State': 'rgba(14, 165, 233, 0.9)', // Sky Blue
            'Vehicle Type > 2': 'rgba(168, 85, 247, 0.9)', // Purple
            'Weekend': 'rgba(236, 72, 153, 0.9)', // Pink
            'Driver Amount Outlier': 'rgba(59, 130, 246, 0.9)', // Blue
            'Rush Hour': 'rgba(34, 197, 94, 0.9)', // Green
            'Amount Unusually High': 'rgba(251, 146, 60, 0.9)', // Orange
            'Route Amount Outlier': 'rgba(139, 92, 246, 0.9)', // Violet
            'Driver Spend Spike': 'rgba(149, 70, 167, 0.9)', // Purple
            'Overlapping Journey': 'rgba(245, 158, 11, 0.9)', // Amber
            'Possible Cloning': 'rgba(168, 85, 247, 0.9)', // Purple
            'Toll Evasion': 'rgba(239, 68, 68, 0.9)', // Red
            'Account Takeover': 'rgba(220, 38, 38, 0.9)', // Dark Red
            'Card Skimming': 'rgba(236, 72, 153, 0.9)', // Pink
            // Gold_automation categories
            'Vehicle Type': 'rgba(168, 85, 247, 0.9)', // Purple
            'Amount > 29': 'rgba(251, 146, 60, 0.9)', // Orange
            'Fraud Detected': 'rgba(220, 38, 38, 0.9)' // Dark Red
        };
        // Fallback colors - using vibrant, distinct colors that are visible in light mode
        const fallbackColors = [
            'rgba(59, 130, 246, 0.9)',   // Blue
            'rgba(34, 197, 94, 0.9)',   // Green
            'rgba(251, 146, 60, 0.9)',  // Orange
            'rgba(139, 92, 246, 0.9)',  // Violet
            'rgba(149, 70, 167, 0.9)',  // Purple
            'rgba(245, 158, 11, 0.9)',  // Amber
            'rgba(239, 68, 68, 0.9)',   // Red
            'rgba(236, 72, 153, 0.9)',  // Pink
            'rgba(168, 85, 247, 0.9)',  // Purple
            'rgba(14, 165, 233, 0.9)'   // Sky Blue
        ];
        return colorMap[category] || fallbackColors[index % fallbackColors.length];
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
        Chart.getChart(chartRef.current)?.destroy();
        chartInstanceRef.current = null;

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
                    borderColor: isDarkMode ? 'rgba(15, 23, 42, 0.8)' : 'rgba(229, 231, 235, 0.9)',
                    borderWidth: 3,
                    hoverOffset: 20,
                    hoverBorderColor: isDarkMode ? '#fff' : '#374151',
                    hoverBorderWidth: 3
                }]
            },
            options: {
                ...getChartOptions(isDarkMode),
                cutout: '65%',
                scales: { x: { display: false }, y: { display: false } },
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: { 
                            color: isDarkMode ? '#e5e7eb' : '#1e293b', 
                            font: { family: "'Inter', sans-serif", size: 12, weight: '500' },
                            padding: 20,
                            usePointStyle: true,
                            pointStyle: 'circle'
                        }
                    },
                    tooltip: {
                        backgroundColor: isDarkMode ? 'rgba(15, 23, 42, 0.95)' : 'rgba(255, 255, 255, 0.95)',
                        titleColor: isDarkMode ? '#f1f5f9' : '#1e293b',
                        bodyColor: isDarkMode ? '#cbd5e1' : '#475569',
                        borderColor: isDarkMode ? '#334155' : '#cbd5e1',
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
    }, [chartData, loading, isDarkMode]);

    if (loading) {
        return (
            <div className="h-80 w-full flex justify-center items-center">
                <div className="text-gray-400 dark:text-gray-400 text-gray-600">Loading chart data...</div>
            </div>
        );
    }

    if (!chartData || chartData.labels.length === 0) {
        return (
            <div className="h-80 w-full flex justify-center items-center">
                <div className="text-gray-400 dark:text-gray-400 text-gray-600">No fraud data available</div>
            </div>
        );
    }

    return <div className="h-80 w-full flex justify-center items-center"><canvas ref={chartRef}></canvas></div>;
};

// Define order for severity levels (highest to lowest)
const severityOrder = ['Critical Risk', 'High Risk', 'Medium Risk', 'Low Risk', 'No Risk'];

const SeverityChart = () => {
    const chartRef = useRef(null);
    const chartInstanceRef = useRef(null);
    const [chartData, setChartData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [isDarkMode, setIsDarkMode] = useState(document.documentElement.classList.contains('dark'));

    useEffect(() => {
        const checkTheme = () => {
            setIsDarkMode(document.documentElement.classList.contains('dark'));
        };
        const observer = new MutationObserver(checkTheme);
        observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });
        checkTheme();
        return () => observer.disconnect();
    }, []);

    // Map backend severity categories to display labels
    const mapSeverityLabel = (severity) => {
        const labelMap = {
            'Critical Risk': 'Critical',
            'High Risk': 'High',
            'Medium Risk': 'Medium',
            'Low Risk': 'Low',
            'No Risk': 'No Risk'
        };
        return labelMap[severity] || severity;
    };

    // Get color for severity level
    const getSeverityColor = (severity) => {
        const colorMap = {
            'Critical Risk': 'rgba(220, 38, 38, 0.8)',      // #DC2626 Red - Urgent
            'High Risk': 'rgba(220, 38, 38, 0.8)',          // #DC2626 Red - Same as Critical
            'Medium Risk': 'rgba(249, 115, 22, 0.8)',       // #F97316 Orange - Warning
            'Low Risk': 'rgba(251, 191, 36, 0.8)',          // #FBBF24 Yellow/Amber - Caution
            'No Risk': 'rgba(16, 185, 129, 0.8)'           // #10B981 Green - Safe
        };
        return colorMap[severity] || 'rgba(156, 163, 175, 0.8)'; // Gray fallback
    };

    useEffect(() => {
        const loadChartData = async () => {
            setLoading(true);
            try {
                const data = await fetchSeverityChartData();
                if (data && data.length > 0) {
                    // Sort by severity order (highest to lowest)
                    const sortedData = [...data].sort((a, b) => {
                        const indexA = severityOrder.indexOf(a.severity);
                        const indexB = severityOrder.indexOf(b.severity);
                        // If not found in order, put at end
                        if (indexA === -1) return 1;
                        if (indexB === -1) return -1;
                        return indexA - indexB;
                    });

                    const labels = sortedData.map(item => mapSeverityLabel(item.severity));
                    const counts = sortedData.map(item => item.count);
                    const colors = sortedData.map(item => getSeverityColor(item.severity));
                    
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
                console.error('Error loading severity chart data:', error);
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
        Chart.getChart(chartRef.current)?.destroy();
        chartInstanceRef.current = null;

        // Don't render chart if no data
        if (chartData.labels.length === 0) {
            return;
        }

        const chart = new Chart(chartRef.current, {
            type: 'doughnut',
            data: {
                labels: chartData.labels,
                datasets: [{
                    label: ' Severity',
                    data: chartData.counts,
                    backgroundColor: chartData.colors,
                    borderColor: isDarkMode ? 'rgba(15, 23, 42, 0.8)' : 'rgba(255, 255, 255, 0.8)',
                    borderWidth: 3,
                    hoverOffset: 20,
                    hoverBorderColor: isDarkMode ? '#fff' : '#1e293b',
                    hoverBorderWidth: 3
                }]
            },
            options: {
                ...getChartOptions(isDarkMode),
                cutout: '65%',
                scales: { x: { display: false }, y: { display: false } },
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: { 
                            color: isDarkMode ? '#e5e7eb' : '#1e293b', 
                            font: { family: "'Inter', sans-serif", size: 12, weight: '500' },
                            padding: 20,
                            usePointStyle: true,
                            pointStyle: 'circle'
                        }
                    },
                    tooltip: {
                        backgroundColor: isDarkMode ? 'rgba(15, 23, 42, 0.95)' : 'rgba(255, 255, 255, 0.95)',
                        titleColor: isDarkMode ? '#f1f5f9' : '#1e293b',
                        bodyColor: isDarkMode ? '#cbd5e1' : '#475569',
                        borderColor: isDarkMode ? '#334155' : '#cbd5e1',
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
    }, [chartData, loading, isDarkMode]);

    if (loading) {
        return (
            <div className="h-80 w-full flex justify-center items-center">
                <div className="text-gray-400 dark:text-gray-400 text-gray-600">Loading chart data...</div>
            </div>
        );
    }

    if (!chartData || chartData.labels.length === 0) {
        return (
            <div className="h-80 w-full flex justify-center items-center">
                <div className="text-gray-400 dark:text-gray-400 text-gray-600">No severity data available</div>
            </div>
        );
    }

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

const formatCurrency = (value) => {
    const numericValue = Number(value) || 0;
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        maximumFractionDigits: 0
    }).format(numericValue);
};

const DashboardView = ({ setActiveView }) => {
    const [animate, setAnimate] = useState(false);
    const [recentFlaggedTransactions, setRecentFlaggedTransactions] = useState([]);
    const [dashboardMetrics, setDashboardMetrics] = useState({
        total_alerts_ytd: 0,
        total_amount: 0,
        detected_frauds_current_month: 0,
        potential_loss_ytd: 0
    });
    const [metricsLoading, setMetricsLoading] = useState(true);
    
    useEffect(() => {
        setAnimate(true);

        const loadRecentTransactions = async () => {
            try {
                const data = await fetchRecentFlaggedTransactions();
                setRecentFlaggedTransactions(data);
            } catch (error) {
                console.error('Error loading recent flagged transactions:', error);
            }
        };

        const loadMetrics = async () => {
            setMetricsLoading(true);
            try {
                const data = await fetchDashboardMetrics();
                setDashboardMetrics({
                    total_alerts_ytd: data?.total_alerts_ytd || 0,
                    total_amount: data?.total_amount || 0,
                    detected_frauds_current_month: data?.detected_frauds_current_month || 0,
                    potential_loss_ytd: data?.potential_loss_ytd || data?.total_amount || 0
                });
            } catch (error) {
                console.error('Error loading dashboard metrics:', error);
                setDashboardMetrics({
                    total_alerts_ytd: 0,
                    total_amount: 0,
                    detected_frauds_current_month: 0,
                    potential_loss_ytd: 0
                });
            } finally {
                setMetricsLoading(false);
            }
        };

        loadRecentTransactions();
        loadMetrics();
    }, []);

    return (
        <div className="space-y-6">
            {/* Stats Cards */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                {/* Total Alerts Card */}
                <div className={`group bg-white dark:bg-white/5 backdrop-blur-md border border-gray-200 dark:border-white/10 p-6 rounded-2xl shadow-xl dark:shadow-[8px_8px_16px_rgba(0,0,0,0.3),-4px_-4px_8px_rgba(255,255,255,0.05)] hover:shadow-2xl dark:hover:shadow-[12px_12px_24px_rgba(0,0,0,0.4),-6px_-6px_12px_rgba(255,255,255,0.08)] hover:border-[#9546A7]/50 dark:hover:border-white/20 transition-all duration-300 transform hover:-translate-y-1 ${animate ? 'animate-fadeIn' : 'opacity-0'}`}>
                    <div className="flex items-center mb-4">
                        <div className="p-3 bg-gradient-to-br from-[#9546A7]/20 to-[#9546A7]/10 rounded-xl ring-2 ring-[#9546A7]/20 group-hover:ring-[#9546A7]/40 transition-all">
                            <ShieldIcon />
                        </div>
                    </div>
                    <h3 className="text-gray-400 dark:text-gray-400 text-gray-600 text-sm font-medium mb-1">Total Alerts (YTD)</h3>
                    <p className="text-4xl font-bold dark:text-white text-gray-900 mb-2 bg-gradient-to-r from-[#9546A7] to-[#B873D1] dark:from-[#9546A7] dark:to-[#B873D1] bg-clip-text text-transparent">
                        {metricsLoading ? '—' : (dashboardMetrics.total_alerts_ytd || 0).toLocaleString()}
                    </p>
                </div>

                {/* Potential Loss Card */}
                <div className={`group bg-white dark:bg-white/5 backdrop-blur-md border border-gray-200 dark:border-white/10 p-6 rounded-2xl shadow-xl dark:shadow-[8px_8px_16px_rgba(0,0,0,0.3),-4px_-4px_8px_rgba(255,255,255,0.05)] hover:shadow-2xl dark:hover:shadow-[12px_12px_24px_rgba(0,0,0,0.4),-6px_-6px_12px_rgba(255,255,255,0.08)] hover:border-rose-500/50 dark:hover:border-white/20 transition-all duration-300 transform hover:-translate-y-1 ${animate ? 'animate-fadeIn' : 'opacity-0'}`} style={{animationDelay: '0.1s'}}>
                    <div className="flex items-center mb-4">
                        <div className="p-3 bg-gradient-to-br from-rose-500/20 to-rose-600/10 rounded-xl ring-2 ring-rose-500/20 group-hover:ring-rose-500/40 transition-all">
                            <DollarIcon />
                        </div>
                    </div>
                    <h3 className="text-gray-400 dark:text-gray-400 text-gray-600 text-sm font-medium mb-1">Potential Loss (YTD)</h3>
                    <p className="text-4xl font-bold dark:text-white text-gray-900 mb-2 bg-gradient-to-r from-rose-400 to-rose-200 dark:from-rose-400 dark:to-rose-200 from-rose-600 to-rose-700 bg-clip-text text-transparent">
                        {metricsLoading ? '—' : formatCurrency(dashboardMetrics.potential_loss_ytd)}
                    </p>
                </div>

                {/* Detected Frauds Card */}
                <div className={`group bg-white dark:bg-white/5 backdrop-blur-md border border-gray-200 dark:border-white/10 p-6 rounded-2xl shadow-xl dark:shadow-[8px_8px_16px_rgba(0,0,0,0.3),-4px_-4px_8px_rgba(255,255,255,0.05)] hover:shadow-2xl dark:hover:shadow-[12px_12px_24px_rgba(0,0,0,0.4),-6px_-6px_12px_rgba(255,255,255,0.08)] hover:border-amber-500/50 dark:hover:border-white/20 transition-all duration-300 transform hover:-translate-y-1 ${animate ? 'animate-fadeIn' : 'opacity-0'}`} style={{animationDelay: '0.2s'}}>
                    <div className="flex items-center mb-4">
                        <div className="p-3 bg-gradient-to-br from-amber-500/20 to-amber-600/10 rounded-xl ring-2 ring-amber-500/20 group-hover:ring-amber-500/40 transition-all">
                            <AlertIcon />
                        </div>
                    </div>
                    <h3 className="text-gray-400 dark:text-gray-400 text-gray-600 text-sm font-medium mb-1">Detected Frauds (Current Month)</h3>
                    <p className="text-4xl font-bold dark:text-white text-gray-900 mb-2 bg-gradient-to-r from-amber-400 to-amber-200 dark:from-amber-400 dark:to-amber-200 from-amber-600 to-amber-700 bg-clip-text text-transparent">
                        {metricsLoading ? '—' : (dashboardMetrics.detected_frauds_current_month || 0).toLocaleString()}
                    </p>
                </div>
            </div>

            {/* Charts Grid */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                {/* Bar Chart */}
                <div className="lg:col-span-2 bg-white dark:bg-white/5 backdrop-blur-md border border-gray-200 dark:border-white/10 p-6 rounded-2xl shadow-xl dark:shadow-[8px_8px_16px_rgba(0,0,0,0.3),-4px_-4px_8px_rgba(255,255,255,0.05)] hover:shadow-2xl dark:hover:shadow-[12px_12px_24px_rgba(0,0,0,0.4),-6px_-6px_12px_rgba(255,255,255,0.08)] transition-all duration-300">
                    <div className="mb-6">
                        <h3 className="font-bold text-xl dark:text-white text-gray-900 mb-1">Transaction Analysis</h3>
                        <p className="text-sm text-gray-400 dark:text-gray-400 text-gray-600">Month-over-month comparison</p>
                    </div>
                    <BarChart />
                </div>

                {/* Alert Feed */}
                <div className="bg-white dark:bg-white/5 backdrop-blur-md border border-gray-200 dark:border-white/10 p-6 rounded-2xl shadow-xl dark:shadow-[8px_8px_16px_rgba(0,0,0,0.3),-4px_-4px_8px_rgba(255,255,255,0.05)] hover:shadow-2xl dark:hover:shadow-[12px_12px_24px_rgba(0,0,0,0.4),-6px_-6px_12px_rgba(255,255,255,0.08)] transition-all duration-300">
                    <div className="flex items-center justify-between mb-6">
                        <h3 className="font-bold text-xl dark:text-white text-gray-900">Recent Flagged Transactions</h3>
                    </div>
                    <div className="space-y-4">
                        {recentFlaggedTransactions.length === 0 ? (
                            <div className="p-4 rounded-xl border border-gray-200 dark:border-white/10 text-center text-sm text-gray-400 dark:text-gray-400">
                                No recent flagged transactions
                            </div>
                        ) : (
                            recentFlaggedTransactions.map((txn) => {
                                // Determine styling based on actual status
                                const isInvestigating = txn.status === 'Investigating' || txn.status === 'Needs Review';
                                return (
                                    <div key={txn.id || txn.transaction_id} className={`p-4 rounded-xl border transition-all duration-300 hover:scale-105 cursor-pointer ${
                                        isInvestigating
                                            ? 'bg-red-500/10 border-red-500/30 hover:border-red-500/50' 
                                            : 'bg-amber-500/10 border-amber-500/30 hover:border-amber-500/50'
                                    }`}>
                                        <div className="flex items-start justify-between">
                                            <div className="flex-1">
                                                <div className="flex items-center justify-between mb-2">
                                                    <span className="font-mono text-xs text-[#9546A7] dark:text-[#9546A7] font-semibold">{txn.id || txn.transaction_id}</span>
                                                    <span className={`px-2 py-0.5 rounded text-xs font-semibold ${
                                                        isInvestigating ? 'bg-red-500/20 text-red-400 dark:text-red-400 text-red-600' : 'bg-amber-500/20 text-amber-400 dark:text-amber-400 text-amber-600'
                                                    }`}>
                                                        {txn.status}
                                                    </span>
                                                </div>
                                                <p className="text-sm dark:text-white text-gray-900 font-medium mb-1">{txn.category || 'Anomaly Detected'}</p>
                                                <div className="flex items-center gap-3 text-xs text-gray-400 dark:text-gray-400 text-gray-600 mb-2">
                                                    <span>{txn.tag_plate_number || txn.tagPlate || '-'}</span>
                                                    <span>•</span>
                                                    <span>${(txn.amount || 0).toFixed(2)}</span>
                                                    <span>•</span>
                                                    <span>{txn.agency || '-'}</span>
                                                </div>
                                                {txn.transaction_date && (
                                                    <span className="text-xs text-gray-400 dark:text-gray-400 text-gray-600">
                                                        {new Date(txn.transaction_date).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}
                                                    </span>
                                                )}
                                            </div>
                                        </div>
                                    </div>
                                );
                            })
                        )}
                        <button 
                            onClick={() => setActiveView('data')}
                            className="w-full py-3 text-sm font-medium text-[#9546A7] dark:text-[#9546A7] hover:text-[#B873D1] dark:hover:text-[#B873D1] hover:text-[#7A3A8F] transition-colors border border-slate-700 dark:border-white/10 border-gray-300 hover:border-[#9546A7]/50 dark:hover:border-white/20 hover:border-[#9546A7] rounded-xl"
                        >
                            View All Alerts →
                        </button>
                    </div>
                </div>
            </div>

            {/* Bottom Charts */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Category Chart */}
                <div className="bg-white dark:bg-white/5 backdrop-blur-md border border-gray-200 dark:border-white/10 p-6 rounded-2xl shadow-xl dark:shadow-[8px_8px_16px_rgba(0,0,0,0.3),-4px_-4px_8px_rgba(255,255,255,0.05)] hover:shadow-2xl dark:hover:shadow-[12px_12px_24px_rgba(0,0,0,0.4),-6px_-6px_12px_rgba(255,255,255,0.08)] transition-all duration-300">
                    <div className="mb-6">
                        <h3 className="font-bold text-xl dark:text-white text-gray-900 mb-1">Fraud by Category</h3>
                        <p className="text-sm text-gray-400 dark:text-gray-400 text-gray-600">Distribution across incident types</p>
                    </div>
                    <CategoryChart />
                </div>

                {/* Severity Chart */}
                <div className="bg-white dark:bg-white/5 backdrop-blur-md border border-gray-200 dark:border-white/10 p-6 rounded-2xl shadow-xl dark:shadow-[8px_8px_16px_rgba(0,0,0,0.3),-4px_-4px_8px_rgba(255,255,255,0.05)] hover:shadow-2xl dark:hover:shadow-[12px_12px_24px_rgba(0,0,0,0.4),-6px_-6px_12px_rgba(255,255,255,0.08)] transition-all duration-300">
                    <div className="mb-6">
                        <h3 className="font-bold text-xl dark:text-white text-gray-900 mb-1">Threat Severity</h3>
                        <p className="text-sm text-gray-400 dark:text-gray-400 text-gray-600">Risk level distribution</p>
                    </div>
                    <SeverityChart />
                </div>
            </div>
        </div>
    );
};

const ScatterPlot = () => {
    const [chartData, setChartData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [isDarkMode, setIsDarkMode] = useState(document.documentElement.classList.contains('dark'));
    const [tableType, setTableType] = useState('master_viz');

    useEffect(() => {
        const checkTheme = () => {
            setIsDarkMode(document.documentElement.classList.contains('dark'));
        };
        const observer = new MutationObserver(checkTheme);
        observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });
        checkTheme();
        return () => observer.disconnect();
    }, []);

    useEffect(() => {
        const fetchScatterData = async () => {
            try {
                // Fetch table info to determine table type
                const tableInfo = await fetchTableInfo();
                setTableType(tableInfo.table_name || 'master_viz');
                
                const response = await fetch(`${apiUrl}/api/charts/scatter`);
                const result = await response.json();
                if (result.data && result.data.length > 0) {
                    setChartData(result.data);
                }
            } catch (error) {
                console.error('Error fetching scatter plot data:', error);
            } finally {
                setLoading(false);
            }
        };
        fetchScatterData();
    }, []);

    if (loading) {
        return <div className="text-gray-400 dark:text-gray-400 text-gray-600">Loading scatter plot data...</div>;
    }

    if (!chartData || chartData.length === 0) {
        return <div className="text-gray-400 dark:text-gray-400 text-gray-600">No data available for scatter plot</div>;
    }

    // Group data by risk level
    const riskLevels = ['Critical Risk', 'High Risk', 'Medium Risk', 'Low Risk'];
    const colorMap = {
        'Critical Risk': 'rgb(220, 38, 38)',      // Red
        'High Risk': 'rgb(255, 140, 0)',          // Orange/Coral
        'Medium Risk': 'rgb(59, 130, 246)',       // Blue
        'Low Risk': 'rgb(34, 197, 94)'            // Green
    };

    const traces = riskLevels.map(riskLevel => {
        const filteredData = chartData.filter(d => d.risk_level === riskLevel);
        return {
            x: filteredData.map(d => d.amount),
            y: filteredData.map(d => d.ml_anomaly_score),
            mode: 'markers',
            type: 'scatter',
            name: riskLevel,
            marker: {
                color: colorMap[riskLevel] || 'gray',
                size: 5,
                opacity: 0.6,
                line: {
                    width: 0.5,
                    color: 'rgba(0, 0, 0, 0.1)'
                }
            }
        };
    }).filter(trace => trace.x.length > 0); // Remove empty traces

    const layout = {
        title: {
            text: tableType === 'gold_automation' ? 'Rule-Based Score vs Amount' : 'ML Anomaly Score vs Amount',
            font: { color: isDarkMode ? '#e5e7eb' : '#1e293b', size: 20 }
        },
        xaxis: {
            title: {
                text: 'Amount',
                font: { color: isDarkMode ? '#94a3b8' : '#64748b' }
            },
            range: [0, 500],
            gridcolor: isDarkMode ? 'rgba(71, 85, 105, 0.2)' : 'rgba(148, 163, 184, 0.2)',
            color: isDarkMode ? '#94a3b8' : '#64748b'
        },
        yaxis: {
            title: {
                text: tableType === 'gold_automation' ? 'Rule-Based Score' : 'ML Anomaly Score',
                font: { color: isDarkMode ? '#94a3b8' : '#64748b' }
            },
            range: [-0.1, 0.5],
            gridcolor: isDarkMode ? 'rgba(71, 85, 105, 0.2)' : 'rgba(148, 163, 184, 0.2)',
            color: isDarkMode ? '#94a3b8' : '#64748b'
        },
        plot_bgcolor: isDarkMode ? 'rgba(15, 23, 42, 0.5)' : 'rgba(255, 255, 255, 0.5)',
        paper_bgcolor: 'transparent',
        font: { color: isDarkMode ? '#94a3b8' : '#64748b' },
        legend: {
            x: 1.02,
            y: 1,
            bgcolor: isDarkMode ? 'rgba(15, 23, 42, 0.8)' : 'rgba(255, 255, 255, 0.8)',
            bordercolor: isDarkMode ? 'rgba(71, 85, 105, 0.5)' : 'rgba(148, 163, 184, 0.5)',
            borderwidth: 1,
            font: { color: isDarkMode ? '#e5e7eb' : '#1e293b' }
        },
        margin: { l: 60, r: 150, t: 60, b: 60 }
    };

    const config = {
        displayModeBar: true,
        displaylogo: false,
        modeBarButtonsToRemove: ['pan2d', 'lasso2d'],
        responsive: true
    };

    return (
        <div style={{ height: '600px' }}>
            <Plot
                key={`${isDarkMode ? 'dark' : 'light'}-${tableType}`}
                data={traces}
                layout={layout}
                config={config}
                style={{ width: '100%', height: '100%' }}
            />
        </div>
    );
};

const ChartsView = () => {
    return (
        <div className="space-y-6">
            <div className="bg-white dark:bg-white/5 backdrop-blur-md border border-gray-200 dark:border-white/10 p-6 rounded-2xl shadow-xl dark:shadow-[8px_8px_16px_rgba(0,0,0,0.3),-4px_-4px_8px_rgba(255,255,255,0.05)] hover:shadow-2xl dark:hover:shadow-[12px_12px_24px_rgba(0,0,0,0.4),-6px_-6px_12px_rgba(255,255,255,0.08)] transition-all duration-300">
                <ScatterPlot />
            </div>
        </div>
    );
};

const DataView = () => {
    const getMLPredictionColor = (category) => {
        if (!category || category === '-') return null;
        const colorMap = {
            // Risk Levels (matching ScatterPlot colors)
            'Critical Risk': 'rgb(220, 38, 38)', // Red
            'High Risk': 'rgb(255, 140, 0)', // Orange/Coral
            'Medium Risk': 'rgb(59, 130, 246)', // Blue
            'Low Risk': 'rgb(34, 197, 94)', // Green
            // Fraud Categories
            'Holiday': 'rgba(239, 68, 68, 1)', // Red
            'Out of State': 'rgba(14, 165, 233, 1)', // Sky Blue
            'Vehicle Type > 2': 'rgba(168, 85, 247, 1)', // Purple
            'Weekend': 'rgba(236, 72, 153, 1)', // Pink
            'Driver Amount Outlier': 'rgba(59, 130, 246, 1)', // Blue
            'Rush Hour': 'rgba(34, 197, 94, 1)', // Green
            'Amount Unusually High': 'rgba(251, 146, 60, 1)', // Orange
            'Route Amount Outlier': 'rgba(139, 92, 246, 1)', // Violet
            'Driver Spend Spike': 'rgba(149, 70, 167, 1)', // Purple
            'Overlapping Journey': 'rgba(245, 158, 11, 1)', // Amber
            'Possible Cloning': 'rgba(168, 85, 247, 1)', // Purple
            'Toll Evasion': 'rgba(239, 68, 68, 1)', // Red
            'Account Takeover': 'rgba(220, 38, 38, 1)', // Dark Red
            'Card Skimming': 'rgba(236, 72, 153, 1)' // Pink
        };
        // Try exact match first
        if (colorMap[category]) return colorMap[category];
        // Try case-insensitive match
        const categoryLower = category.toLowerCase();
        for (const [key, value] of Object.entries(colorMap)) {
            if (key.toLowerCase() === categoryLower) return value;
        }
        // Check if it contains risk level keywords
        if (categoryLower.includes('critical')) return 'rgb(220, 38, 38)'; // Red
        if (categoryLower.includes('high')) return 'rgb(255, 140, 0)'; // Orange
        if (categoryLower.includes('medium')) return 'rgb(59, 130, 246)'; // Blue
        if (categoryLower.includes('low')) return 'rgb(34, 197, 94)'; // Green
        // Fallback to a default color
        return 'rgba(149, 70, 167, 1)'; // Default purple
    };

    const [searchTerm, setSearchTerm] = useState('');
    const [filterStatus, setFilterStatus] = useState('all');
    const [filterMLCategory, setFilterMLCategory] = useState('all');
    const [sortColumn, setSortColumn] = useState(null);
    const [sortDirection, setSortDirection] = useState('asc'); // 'asc' or 'desc'
    const [transactionData, setTransactionData] = useState([]);
    const [loading, setLoading] = useState(true);
    const [currentPage, setCurrentPage] = useState(1);
    const [totalCount, setTotalCount] = useState(0);
    const itemsPerPage = 50; // rows per page (increased for better performance)
    const STATUS_TRANSITIONS = {
        "No Action Required": [],   // cannot change
        "Needs Review": ["Resolved - Not Fraud", "Investigating", "Resolved - Fraud"],
        "Investigating": ["Resolved - Not Fraud", "Resolved - Fraud"],
        "Resolved - Not Fraud": [],             // cannot change
        "Resolved - Fraud": []               // cannot change
    };

    // Detect table type from data
    const detectTableType = (data) => {
        if (!data || data.length === 0) return 'master_viz'; // default
        const firstRow = data[0];
        // gold_automation has threat_severity, master_viz has ml_predicted_category
        if (firstRow.hasOwnProperty('threat_severity')) {
            return 'gold_automation';
        }
        return 'master_viz';
    };

    const [tableType, setTableType] = useState('master_viz');
    const [searchDebounce, setSearchDebounce] = useState('');

    // Debounce search input
    useEffect(() => {
        const timer = setTimeout(() => {
            setSearchDebounce(searchTerm);
        }, 500); // 500ms debounce

        return () => clearTimeout(timer);
    }, [searchTerm]);

    // Fetch data when page, search, or filters change
    useEffect(() => {
        const loadData = async () => {
            setLoading(true);
            try {
                // Fetch data and count in parallel
                const [data, count] = await Promise.all([
                    fetchTransactions(currentPage, itemsPerPage, searchDebounce, filterStatus, filterMLCategory),
                    fetchTransactionsCount(searchDebounce, filterStatus, filterMLCategory)
                ]);
                
                setTransactionData(data);
                setTotalCount(count);
                
                // Detect table type from the data
                if (data && data.length > 0) {
                    setTableType(detectTableType(data));
                }
            } catch (error) {
                console.error('Error loading transactions:', error);
                setTransactionData([]);
                setTotalCount(0);
            } finally {
                setLoading(false);
            }
        };
        loadData();
    }, [currentPage, searchDebounce, filterStatus, filterMLCategory]);

    // Reset to page 1 when search or filters change
    useEffect(() => {
        setCurrentPage(1);
    }, [searchDebounce, filterStatus, filterMLCategory]);

    const handleStatusChange = (transactionId, newStatus) => {
        setTransactionData(prev =>
            prev.map(t => {
                if (t.transaction_id !== transactionId && t.id !== transactionId) return t;

                const allowed = STATUS_TRANSITIONS[t.status] || [];
                if (!allowed.includes(newStatus)) return t; // ignore invalid change

                return { ...t, status: newStatus };
            })
        );

        // also send to backend
        updateTransactionStatus(transactionId, newStatus);
    };
    const updateTransactionStatus = async (transactionId, newStatus) => {
        
        await fetch(`${apiUrl}/api/transactions/update-status`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ transactionId, newStatus }),
        });
    };



    const handleSort = (column) => {
        if (sortColumn === column) {
            // Toggle direction if same column
            setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
        } else {
            // New column, start with ascending
            setSortColumn(column);
            setSortDirection('asc');
        }
    };

    // Helper function to render sortable column header
    const SortableHeader = ({ column, label, minWidthClass = 'min-w-[120px]' }) => (
        <th 
            scope="col" 
            className={`px-4 py-4 text-left text-sm font-semibold text-gray-400 dark:text-gray-400 text-gray-600 uppercase tracking-wider whitespace-nowrap ${minWidthClass} cursor-pointer hover:bg-slate-800/50 dark:hover:bg-slate-800/50 hover:bg-gray-100 transition-colors`}
            onClick={() => handleSort(column)}
        >
            <div className="flex items-center gap-2">
                {label}
                {sortColumn === column ? (
                    sortDirection === 'asc' ? (
                        <SortUpIcon />
                    ) : (
                        <SortDownIcon />
                    )
                ) : (
                    <span className="text-gray-600 dark:text-gray-600 text-gray-400"><SortIcon /></span>
                )}
            </div>
        </th>
    );

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

    // Client-side sorting for current page only (server handles filtering and pagination)
    let sortedData = [...transactionData];
    if (sortColumn) {
        sortedData = sortedData.sort((a, b) => {
            let aValue, bValue;
   
            // Numeric columns
            const numericColumns = ['amount', 'ml_predicted_score', 'rule_based_score', 'distance_miles', 
                                   'travel_time_minutes', 'speed_mph', 'plan_rate'];
            if (numericColumns.includes(sortColumn)) {
                aValue = a[sortColumn] !== null && a[sortColumn] !== undefined ? parseFloat(a[sortColumn]) : -Infinity;
                bValue = b[sortColumn] !== null && b[sortColumn] !== undefined ? parseFloat(b[sortColumn]) : -Infinity;
                if (aValue === bValue) return 0;
                const comparison = aValue > bValue ? 1 : -1;
                return sortDirection === 'asc' ? comparison : -comparison;
            }
            
            // Date columns
            const dateColumns = ['transaction_date', 'posting_date', 'entry_time', 'exit_time', 
                                'prediction_timestamp', 'last_updated'];
            if (dateColumns.includes(sortColumn)) {
                aValue = a[sortColumn] ? new Date(a[sortColumn]).getTime() : -Infinity;
                bValue = b[sortColumn] ? new Date(b[sortColumn]).getTime() : -Infinity;
                if (aValue === bValue) return 0;
                const comparison = aValue > bValue ? 1 : -1;
                return sortDirection === 'asc' ? comparison : -comparison;
            }
            
            // Boolean columns
            const booleanColumns = ['is_anomaly', 'flag_fraud', 'route_instate', 'is_impossible_travel', 'is_rapid_succession',
                                   'flag_rush_hour', 'flag_is_weekend', 'flag_is_holiday', 'flag_overlapping_journey',
                                   'flag_driver_amount_outlier', 'flag_route_amount_outlier', 
                                   'flag_amount_unusually_high', 'flag_driver_spend_spike',
                                   'flag_vehicle_type', 'flag_amount_gt_29', 'flag_is_out_of_state'];
            if (booleanColumns.includes(sortColumn)) {
                aValue = a[sortColumn] === true ? 1 : (a[sortColumn] === false ? 0 : -1);
                bValue = b[sortColumn] === true ? 1 : (b[sortColumn] === false ? 0 : -1);
                if (aValue === bValue) return 0;
                const comparison = aValue > bValue ? 1 : -1;
                return sortDirection === 'asc' ? comparison : -comparison;
            }
            
            // String columns (default)
            aValue = (a[sortColumn] || '').toString().toLowerCase();
            bValue = (b[sortColumn] || '').toString().toLowerCase();
            if (aValue === bValue) return 0;
            const comparison = aValue > bValue ? 1 : -1;
            return sortDirection === 'asc' ? comparison : -comparison;
        });
    }

    // Data is already paginated from server
    const paginatedData = sortedData;

    // Total pages based on server count
    const totalPages = Math.ceil(totalCount / itemsPerPage);



    if (loading) return <div>Loading transactions...</div>;
    return (
        <div className="space-y-6">
            {/* Search and Filter Bar */}
            <div className="bg-white dark:bg-white/5 backdrop-blur-md border border-gray-200 dark:border-white/10 p-6 rounded-2xl shadow-xl dark:shadow-[8px_8px_16px_rgba(0,0,0,0.3),-4px_-4px_8px_rgba(255,255,255,0.05)]">
                <div className="flex flex-col md:flex-row gap-4">
                    <div className="flex-1 relative">
                        <input
                            type="text"
                            placeholder="Search transactions..."
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                            className="w-full bg-white dark:bg-white/5 dark:backdrop-blur-md border border-gray-300 dark:border-white/10 rounded-xl px-4 py-3 pl-11 dark:text-white text-gray-900 placeholder-gray-500 dark:placeholder-gray-500 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-indigo-500/50 focus:border-indigo-500/50 dark:focus:border-white/20 transition-all dark:shadow-[4px_4px_8px_rgba(0,0,0,0.2),-2px_-2px_4px_rgba(255,255,255,0.03)]"
                        />
                        <div className="absolute left-3 top-3.5 text-gray-500 dark:text-gray-500 text-gray-400">
                            <SearchIcon />
                        </div>
                    </div>
                    <div className="flex space-x-2">
                        <select
                            value={filterStatus}
                            onChange={(e) => setFilterStatus(e.target.value)}
                            className="px-4 py-3 rounded-xl text-sm font-medium bg-white dark:bg-white/5 dark:backdrop-blur-md text-gray-700 dark:text-gray-300 border border-gray-300 dark:border-white/10 focus:outline-none focus:ring-2 focus:ring-indigo-500/50 focus:border-indigo-500/50 dark:focus:border-white/20 transition-all cursor-pointer dark:shadow-[4px_4px_8px_rgba(0,0,0,0.2),-2px_-2px_4px_rgba(255,255,255,0.03)]"
                        >
                            <option value="all" className="bg-slate-800 dark:bg-white/5 bg-white">All Status</option>
                            <option value="Resolved - Fraud" className="bg-slate-800 dark:bg-white/5 bg-white">Resolved - Fraud</option>
                            <option value="Investigating" className="bg-slate-800 dark:bg-white/5 bg-white">Investigating</option>
                            <option value="Needs Review" className="bg-slate-800 dark:bg-white/5 bg-white">Needs Review</option>
                            <option value="Resolved - Not Fraud" className="bg-slate-800 dark:bg-white/5 bg-white">Resolved - Not Fraud</option>
                            <option value="No Action Required" className="bg-slate-800 dark:bg-white/5 bg-white">No Action Required</option>
                        </select>
                        <select
                            value={filterMLCategory}
                            onChange={(e) => setFilterMLCategory(e.target.value)}
                            className="px-4 py-3 rounded-xl text-sm font-medium bg-white dark:bg-white/5 dark:backdrop-blur-md text-gray-700 dark:text-gray-300 border border-gray-300 dark:border-white/10 focus:outline-none focus:ring-2 focus:ring-indigo-500/50 focus:border-indigo-500/50 dark:focus:border-white/20 transition-all cursor-pointer dark:shadow-[4px_4px_8px_rgba(0,0,0,0.2),-2px_-2px_4px_rgba(255,255,255,0.03)]"
                        >
                            <option value="all" className="bg-slate-800 dark:bg-white/5 bg-white">All Risk Levels</option>
                            <option value="Critical Risk" className="bg-slate-800 dark:bg-white/5 bg-white">Critical Risk</option>
                            <option value="High Risk" className="bg-slate-800 dark:bg-white/5 bg-white">High Risk</option>
                            <option value="Medium Risk" className="bg-slate-800 dark:bg-white/5 bg-white">Medium Risk</option>
                            <option value="Low Risk" className="bg-slate-800 dark:bg-white/5 bg-white">Low Risk</option>
                        </select>
                    </div>
                </div>
            </div>

            {/* Data Table */}
            <div className="bg-white dark:bg-white/5 backdrop-blur-md border border-gray-200 dark:border-white/10 rounded-2xl shadow-xl dark:shadow-[8px_8px_16px_rgba(0,0,0,0.3),-4px_-4px_8px_rgba(255,255,255,0.05)] overflow-hidden">
                <div className="overflow-x-auto">
                    <table className="min-w-full text-base table-auto">
                        <thead className="bg-white dark:bg-white/5 border-b border-gray-300 dark:border-white/10">
                            <tr>
                                <SortableHeader column="status" label="Status" minWidthClass="min-w-[120px]" />
                                {/* Conditional header based on table type */}
                                {tableType === 'gold_automation' ? (
                                    <SortableHeader column="threat_severity" label="Threat Severity" minWidthClass="min-w-[150px]" />
                                ) : (
                                    <SortableHeader column="ml_predicted_category" label="ML Prediction" minWidthClass="min-w-[150px]" />
                                )}
                                {/* Conditional anomaly/fraud header */}
                                {tableType === 'gold_automation' ? (
                                    <SortableHeader column="flag_fraud" label="Flag Fraud" minWidthClass="min-w-[100px]" />
                                ) : (
                                    <SortableHeader column="is_anomaly" label="Is Anomaly" minWidthClass="min-w-[100px]" />
                                )}
                                <SortableHeader column="rule_based_score" label="Rule-Based Score" minWidthClass="min-w-[120px]" />
                                {/* ML Predicted Score only for master_viz */}
                                {tableType === 'master_viz' && (
                                    <SortableHeader column="ml_predicted_score" label="ML Predicted Score" minWidthClass="min-w-[120px]" />
                                )}
                                <SortableHeader column="amount" label="Amount" minWidthClass="min-w-[100px]" />
                                <SortableHeader column="transaction_id" label="Transaction ID" minWidthClass="min-w-[120px]" />
                                <SortableHeader column="tag_plate_number" label="Tag/Plate Number" minWidthClass="min-w-[140px]" />
                                <SortableHeader column="transaction_date" label="Transaction Date" minWidthClass="min-w-[120px]" />
                                {/* Posting Date only for master_viz */}
                                {tableType === 'master_viz' && (
                                    <SortableHeader column="posting_date" label="Posting Date" minWidthClass="min-w-[120px]" />
                                )}
                                <SortableHeader column="agency" label="Agency" minWidthClass="min-w-[100px]" />
                                {/* Agency Name only for gold_automation */}
                                {tableType === 'gold_automation' && (
                                    <SortableHeader column="agency_name" label="Agency Name" minWidthClass="min-w-[120px]" />
                                )}
                                <SortableHeader column="state_name" label="State" minWidthClass="min-w-[100px]" />
                                {/* Route Name only for master_viz */}
                                {tableType === 'master_viz' && (
                                    <>
                                        <SortableHeader column="route_name" label="Route Name" minWidthClass="min-w-[120px]" />
                                        <SortableHeader column="route_instate" label="Location Scope" minWidthClass="min-w-[100px]" />
                                    </>
                                )}
                                <SortableHeader column="entry_time" label="Entry Time" minWidthClass="min-w-[150px]" />
                                <SortableHeader column="entry_plaza" label="Entry Plaza" minWidthClass="min-w-[100px]" />
                                {/* Entry Plaza Name only for gold_automation */}
                                {tableType === 'gold_automation' && (
                                    <SortableHeader column="entry_plaza_name" label="Entry Plaza Name" minWidthClass="min-w-[140px]" />
                                )}
                                {/* Entry Lane only for master_viz */}
                                {tableType === 'master_viz' && (
                                    <SortableHeader column="entry_lane" label="Entry Lane" minWidthClass="min-w-[100px]" />
                                )}
                                <SortableHeader column="exit_time" label="Exit Time" minWidthClass="min-w-[150px]" />
                                <SortableHeader column="exit_plaza" label="Exit Plaza" minWidthClass="min-w-[100px]" />
                                {/* Exit Plaza Name only for gold_automation */}
                                {tableType === 'gold_automation' && (
                                    <SortableHeader column="exit_plaza_name" label="Exit Plaza Name" minWidthClass="min-w-[140px]" />
                                )}
                                {/* Exit Lane only for master_viz */}
                                {tableType === 'master_viz' && (
                                    <SortableHeader column="exit_lane" label="Exit Lane" minWidthClass="min-w-[100px]" />
                                )}
                                {/* Vehicle Type Code only for gold_automation */}
                                {tableType === 'gold_automation' && (
                                    <SortableHeader column="vehicle_type_code" label="Vehicle Type Code" minWidthClass="min-w-[120px]" />
                                )}
                                <SortableHeader column="vehicle_type_name" label="Vehicle Type" minWidthClass="min-w-[120px]" />
                                {/* Plan Rate and Fare Type only for master_viz */}
                                {tableType === 'master_viz' && (
                                    <>
                                        <SortableHeader column="plan_rate" label="Plan Rate" minWidthClass="min-w-[100px]" />
                                        <SortableHeader column="fare_type" label="Fare Type" minWidthClass="min-w-[100px]" />
                                    </>
                                )}
                                {/* Distance, Travel Time, Speed only for master_viz */}
                                {tableType === 'master_viz' && (
                                    <>
                                        <SortableHeader column="distance_miles" label="Distance (mi)" minWidthClass="min-w-[100px]" />
                                        <SortableHeader column="travel_time_minutes" label="Travel Time (min)" minWidthClass="min-w-[120px]" />
                                        <SortableHeader column="speed_mph" label="Speed (mph)" minWidthClass="min-w-[100px]" />
                                        <SortableHeader column="travel_time_category" label="Travel Category" minWidthClass="min-w-[120px]" />
                                        <SortableHeader column="is_impossible_travel" label="Impossible Travel" minWidthClass="min-w-[100px]" />
                                        <SortableHeader column="is_rapid_succession" label="Rapid Succession" minWidthClass="min-w-[120px]" />
                                        <SortableHeader column="flag_rush_hour" label="Rush Hour" minWidthClass="min-w-[100px]" />
                                        <SortableHeader column="flag_overlapping_journey" label="Overlapping Journey" minWidthClass="min-w-[140px]" />
                                        <SortableHeader column="flag_driver_amount_outlier" label="Driver Amount Outlier" minWidthClass="min-w-[140px]" />
                                        <SortableHeader column="flag_route_amount_outlier" label="Route Amount Outlier" minWidthClass="min-w-[140px]" />
                                        <SortableHeader column="flag_amount_unusually_high" label="Amount Unusually High" minWidthClass="min-w-[140px]" />
                                        <SortableHeader column="flag_driver_spend_spike" label="Driver Spend Spike" minWidthClass="min-w-[140px]" />
                                        <SortableHeader column="prediction_timestamp" label="Prediction Timestamp" minWidthClass="min-w-[150px]" />
                                    </>
                                )}
                                {/* Gold automation specific flags */}
                                {tableType === 'gold_automation' && (
                                    <>
                                        <SortableHeader column="flag_vehicle_type" label="Flag Vehicle Type" minWidthClass="min-w-[140px]" />
                                        <SortableHeader column="flag_amount_gt_29" label="Flag Amount > 29" minWidthClass="min-w-[140px]" />
                                        <SortableHeader column="flag_is_out_of_state" label="Flag Out of State" minWidthClass="min-w-[140px]" />
                                    </>
                                )}
                                <SortableHeader column="flag_is_weekend" label="Weekend" minWidthClass="min-w-[100px]" />
                                <SortableHeader column="flag_is_holiday" label="Holiday" minWidthClass="min-w-[100px]" />
                                {/* Last Updated only for master_viz */}
                                {tableType === 'master_viz' && (
                                    <SortableHeader column="last_updated" label="Last Updated" minWidthClass="min-w-[150px]" />
                                )}
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-700/50 dark:divide-slate-700/50 divide-gray-200">
                            {paginatedData.map((row, index) => {
                                const transactionId = formatValue(row.transaction_id);
                                const tagPlate = formatValue(row.tag_plate_number || row.tagPlate || row.tag_plate || row.plate_number);
                                const transactionDate = formatDate(row.transaction_date);
                                const postingDate = formatDate(row.posting_date);
                                const agency = formatValue(row.agency);
                                const stateName = formatValue(row.state_name);
                                const routeName = formatValue(row.route_name);
                                const routeInstate = row.route_instate !== null && row.route_instate !== undefined ? (row.route_instate ? 'In-State' : 'Out-State') : '-';
                                const entryTime = formatDateTime(row.entryTime || row.entry_time);
                                const entryPlaza = formatValue(row.entryPlaza || row.entry_plaza);
                                const entryLane = formatValue(row.entry_lane);
                                const exitTime = formatDateTime(row.exitTime || row.exit_time);
                                const exitPlaza = formatValue(row.exitPlaza || row.exit_plaza);
                                const exitLane = formatValue(row.exit_lane);
                                const vehicleType = formatValue(row.vehicle_type_name);
                                const planRate = row.plan_rate !== null && row.plan_rate !== undefined ? `$${parseFloat(row.plan_rate).toFixed(2)}` : '-';
                                const fareType = formatValue(row.fare_type);
                                const amount = row.amount !== null && row.amount !== undefined ? `$${parseFloat(row.amount).toFixed(2)}` : '-';
                                const distanceMiles = row.distance_miles !== null && row.distance_miles !== undefined ? parseFloat(row.distance_miles).toFixed(2) : '-';
                                const travelTimeMinutes = row.travel_time_minutes !== null && row.travel_time_minutes !== undefined ? parseFloat(row.travel_time_minutes).toFixed(1) : '-';
                                const speedMph = row.speed_mph !== null && row.speed_mph !== undefined ? parseFloat(row.speed_mph).toFixed(1) : '-';
                                const travelCategory = formatValue(row.travel_time_category);
                                // Table type specific values
                                const isAnomaly = row.is_anomaly !== null && row.is_anomaly !== undefined ? (row.is_anomaly ? 'Yes' : 'No') : '-';
                                const flagFraud = row.flag_fraud !== null && row.flag_fraud !== undefined ? (row.flag_fraud ? 'Yes' : 'No') : '-';
                                const ruleBasedScore = row.rule_based_score !== null && row.rule_based_score !== undefined ? parseFloat(row.rule_based_score).toFixed(2) : '-';
                                const mlPredictedScore = row.ml_predicted_score !== null && row.ml_predicted_score !== undefined ? parseFloat(row.ml_predicted_score).toFixed(4) : '-';
                                const mlPredictedCategory = formatValue(row.ml_predicted_category);
                                const threatSeverity = formatValue(row.threat_severity);
                                const status = formatValue(row.status);
                                // Gold automation specific values
                                const agencyName = formatValue(row.agency_name);
                                const entryPlazaName = formatValue(row.entry_plaza_name);
                                const exitPlazaName = formatValue(row.exit_plaza_name);
                                const vehicleTypeCode = formatValue(row.vehicle_type_code);
                                const flagVehicleType = row.flag_vehicle_type !== null && row.flag_vehicle_type !== undefined ? (row.flag_vehicle_type ? 'Yes' : 'No') : '-';
                                const flagAmountGt29 = row.flag_amount_gt_29 !== null && row.flag_amount_gt_29 !== undefined ? (row.flag_amount_gt_29 ? 'Yes' : 'No') : '-';
                                const flagIsOutOfState = row.flag_is_out_of_state !== null && row.flag_is_out_of_state !== undefined ? (row.flag_is_out_of_state ? 'Yes' : 'No') : '-';
                                const isImpossibleTravel = row.is_impossible_travel !== null && row.is_impossible_travel !== undefined ? (row.is_impossible_travel ? 'Yes' : 'No') : '-';
                                const isRapidSuccession = row.is_rapid_succession !== null && row.is_rapid_succession !== undefined ? (row.is_rapid_succession ? 'Yes' : 'No') : '-';
                                const flagRushHour = row.flag_rush_hour !== null && row.flag_rush_hour !== undefined ? (row.flag_rush_hour ? 'Yes' : 'No') : '-';
                                const flagIsWeekend = row.flag_is_weekend !== null && row.flag_is_weekend !== undefined ? (row.flag_is_weekend ? 'Yes' : 'No') : '-';
                                const flagIsHoliday = row.flag_is_holiday !== null && row.flag_is_holiday !== undefined ? (row.flag_is_holiday ? 'Yes' : 'No') : '-';
                                const flagOverlappingJourney = row.flag_overlapping_journey !== null && row.flag_overlapping_journey !== undefined ? (row.flag_overlapping_journey ? 'Yes' : 'No') : '-';
                                const flagDriverAmountOutlier = row.flag_driver_amount_outlier !== null && row.flag_driver_amount_outlier !== undefined ? (row.flag_driver_amount_outlier ? 'Yes' : 'No') : '-';
                                const flagRouteAmountOutlier = row.flag_route_amount_outlier !== null && row.flag_route_amount_outlier !== undefined ? (row.flag_route_amount_outlier ? 'Yes' : 'No') : '-';
                                const flagAmountUnusuallyHigh = row.flag_amount_unusually_high !== null && row.flag_amount_unusually_high !== undefined ? (row.flag_amount_unusually_high ? 'Yes' : 'No') : '-';
                                const flagDriverSpendSpike = row.flag_driver_spend_spike !== null && row.flag_driver_spend_spike !== undefined ? (row.flag_driver_spend_spike ? 'Yes' : 'No') : '-';
                                const predictionTimestamp = formatDateTime(row.prediction_timestamp);
                                const lastUpdated = formatDateTime(row.last_updated);

                                return (
                                    <tr 
                                        key={row.transaction_id || row.id || index} 
                                        className="hover:bg-slate-700/30 dark:hover:bg-slate-700/30 hover:bg-gray-100 transition-colors duration-150 group"
                                    >
                                        <td className="px-4 py-4 whitespace-nowrap">
                                            {status !== '-' ? (
                                                <select
                                                    value={status}
                                                    onChange={(e) => handleStatusChange(row.transaction_id || row.id, e.target.value)}
                                                    disabled={STATUS_TRANSITIONS[status].length === 0}   // disable if no edits allowed
                                                    className={`px-2 py-1.5 rounded-lg text-xs font-semibold border-0 cursor-pointer 
                                                    ${STATUS_TRANSITIONS[status].length === 0 ? "opacity-50 cursor-not-allowed" : ""}
                                                    `}
                                                >
                                                    {/* Current status appears first */}
                                                    <option value={status}>{status}</option>

                                                    {/* Only show allowed new statuses */}
                                                    {STATUS_TRANSITIONS[status].map(option => (
                                                        <option key={option} value={option}>{option}</option>
                                                    ))}
                                                </select>
                                            ) : (
                                                <span className="text-gray-400 dark:text-gray-400 text-gray-600 text-base">-</span>
                                            )}
                                        </td>
                                        {/* ML Prediction / Threat Severity */}
                                        <td className="px-4 py-4 whitespace-nowrap">
                                            {tableType === 'gold_automation' ? (
                                                threatSeverity !== '-' ? (
                                                    <span className={`inline-block px-2 py-1 rounded-lg text-xs font-semibold ${
                                                        threatSeverity?.toLowerCase().includes('critical') ? 'bg-red-500/20 text-red-400 ring-1 ring-red-500/30' :
                                                        threatSeverity?.toLowerCase().includes('high') ? 'bg-orange-500/20 text-orange-400 ring-1 ring-orange-500/30' :
                                                        threatSeverity?.toLowerCase().includes('medium') ? 'bg-yellow-500/20 text-yellow-400 ring-1 ring-yellow-500/30' :
                                                        'bg-green-500/20 text-green-400 ring-1 ring-green-500/30'
                                                    }`}>
                                                        {threatSeverity}
                                                    </span>
                                                ) : (
                                                    <span className="text-gray-400 dark:text-gray-400 text-gray-600 text-base">-</span>
                                                )
                                            ) : (
                                                mlPredictedCategory !== '-' ? (
                                                    <span className={`inline-block px-2 py-1 rounded-lg text-xs font-semibold ${
                                                        mlPredictedCategory?.toLowerCase().includes('critical') ? 'bg-red-500/20 text-red-400 ring-1 ring-red-500/30' :
                                                        mlPredictedCategory?.toLowerCase().includes('high') ? 'bg-orange-500/20 text-orange-400 ring-1 ring-orange-500/30' :
                                                        mlPredictedCategory?.toLowerCase().includes('medium') ? 'bg-yellow-500/20 text-yellow-400 ring-1 ring-yellow-500/30' :
                                                        'bg-green-500/20 text-green-400 ring-1 ring-green-500/30'
                                                    }`}>
                                                        {mlPredictedCategory}
                                                    </span>
                                                ) : (
                                                    <span className="text-gray-400 dark:text-gray-400 text-gray-600 text-base">-</span>
                                                )
                                            )}
                                        </td>
                                        {/* Is Anomaly / Flag Fraud */}
                                        <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                            {tableType === 'gold_automation' ? flagFraud : isAnomaly}
                                        </td>
                                        <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                            {ruleBasedScore}
                                        </td>
                                        {/* ML Predicted Score only for master_viz */}
                                        {tableType === 'master_viz' && (
                                            <td className="px-4 py-4 whitespace-nowrap text-base">
                                                <span 
                                                    style={{ color: getMLPredictionColor(mlPredictedCategory) || 'inherit' }}
                                                >
                                                    {mlPredictedScore}
                                                </span>
                                            </td>
                                        )}
                                        <td className="px-4 py-4 whitespace-nowrap">
                                            <span className="font-semibold text-black dark:text-white text-base">{amount}</span>
                                        </td>
                                        <td className="px-4 py-4 whitespace-nowrap">
                                            <span className="font-mono text-gray-100 dark:text-gray-100 text-gray-800 font-medium text-sm">{transactionId}</span>
                                        </td>
                                        <td className="px-4 py-4 whitespace-nowrap">
                                            <span className="font-mono text-gray-100 dark:text-gray-100 text-gray-800 font-medium text-base">{tagPlate}</span>
                                        </td>
                                        <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                            {transactionDate}
                                        </td>
                                        {/* Posting Date only for master_viz */}
                                        {tableType === 'master_viz' && (
                                            <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                {postingDate}
                                            </td>
                                        )}
                                        <td className="px-4 py-4 whitespace-nowrap">
                                            {agency !== '-' ? (
                                                <span className={`inline-block px-2 py-1 rounded-lg text-xs font-semibold ${
                                                    agency === 'PANYNJ' ? 'bg-blue-500/20 text-blue-400 ring-1 ring-blue-500/30' :
                                                    'bg-purple-500/20 text-purple-400 ring-1 ring-purple-500/30'
                                                }`}>
                                                    {agency}
                                                </span>
                                            ) : (
                                                <span className="text-gray-400 dark:text-gray-400 text-gray-600 text-base">-</span>
                                            )}
                                        </td>
                                        {/* Agency Name only for gold_automation */}
                                        {tableType === 'gold_automation' && (
                                            <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                {agencyName}
                                            </td>
                                        )}
                                        <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                            {stateName}
                                        </td>
                                        {/* Route Name only for master_viz */}
                                        {tableType === 'master_viz' && (
                                            <>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {routeName}
                                                </td>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {routeInstate}
                                                </td>
                                            </>
                                        )}
                                        <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                            {entryTime}
                                        </td>
                                        <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                            {entryPlaza}
                                        </td>
                                        {/* Entry Plaza Name only for gold_automation */}
                                        {tableType === 'gold_automation' && (
                                            <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                {entryPlazaName}
                                            </td>
                                        )}
                                        {/* Entry Lane only for master_viz */}
                                        {tableType === 'master_viz' && (
                                            <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                {entryLane}
                                            </td>
                                        )}
                                        <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                            {exitTime}
                                        </td>
                                        <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                            {exitPlaza}
                                        </td>
                                        {/* Exit Plaza Name only for gold_automation */}
                                        {tableType === 'gold_automation' && (
                                            <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                {exitPlazaName}
                                            </td>
                                        )}
                                        {/* Exit Lane only for master_viz */}
                                        {tableType === 'master_viz' && (
                                            <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                {exitLane}
                                            </td>
                                        )}
                                        {/* Vehicle Type Code only for gold_automation */}
                                        {tableType === 'gold_automation' && (
                                            <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                {vehicleTypeCode}
                                            </td>
                                        )}
                                        <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                            {vehicleType}
                                        </td>
                                        {/* Plan Rate and Fare Type only for master_viz */}
                                        {tableType === 'master_viz' && (
                                            <>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {planRate}
                                                </td>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {fareType}
                                                </td>
                                            </>
                                        )}
                                        {/* Distance, Travel Time, Speed only for master_viz */}
                                        {tableType === 'master_viz' && (
                                            <>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {distanceMiles}
                                                </td>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {travelTimeMinutes}
                                                </td>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {speedMph}
                                                </td>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {travelCategory}
                                                </td>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {isImpossibleTravel}
                                                </td>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {isRapidSuccession}
                                                </td>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {flagRushHour}
                                                </td>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {flagOverlappingJourney}
                                                </td>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {flagDriverAmountOutlier}
                                                </td>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {flagRouteAmountOutlier}
                                                </td>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {flagAmountUnusuallyHigh}
                                                </td>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {flagDriverSpendSpike}
                                                </td>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {predictionTimestamp}
                                                </td>
                                            </>
                                        )}
                                        {/* Gold automation specific flags */}
                                        {tableType === 'gold_automation' && (
                                            <>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {flagVehicleType}
                                                </td>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {flagAmountGt29}
                                                </td>
                                                <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                    {flagIsOutOfState}
                                                </td>
                                            </>
                                        )}
                                        <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                            {flagIsWeekend}
                                        </td>
                                        <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                            {flagIsHoliday}
                                        </td>
                                        {/* Last Updated only for master_viz */}
                                        {tableType === 'master_viz' && (
                                            <td className="px-4 py-4 whitespace-nowrap text-gray-300 dark:text-gray-300 text-gray-700 text-base">
                                                {lastUpdated}
                                            </td>
                                        )}
                                    </tr>
                                );
                            })}
                        </tbody>
                    </table>
                </div>
                
                {/* Pagination */}
                <div className="bg-white dark:bg-white/5 px-6 py-4 border-t border-gray-300 dark:border-white/10 flex flex-col sm:flex-row items-center justify-between gap-4">
                    <div className="text-sm text-gray-400 dark:text-gray-400 text-gray-600">
                        Showing <span className="font-semibold dark:text-white text-gray-900">
                            {paginatedData.length > 0 ? (currentPage - 1) * itemsPerPage + 1 : 0}
                        </span> to <span className="font-semibold dark:text-white text-gray-900">
                            {Math.min(currentPage * itemsPerPage, totalCount)}
                        </span> of <span className="font-semibold dark:text-white text-gray-900">{totalCount}</span> transactions
                    </div>
                    <div className="flex items-center space-x-2">
                        <button 
                            onClick={() => setCurrentPage(p => Math.max(p - 1, 1))}
                            disabled={currentPage === 1 || totalPages === 0}
                            className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                                currentPage === 1 || totalPages === 0
                                    ? 'bg-slate-700/30 dark:bg-white/5 dark:backdrop-blur-md dark:shadow-[inset_2px_2px_4px_rgba(0,0,0,0.2),inset_-1px_-1px_2px_rgba(255,255,255,0.03)] bg-gray-200 text-gray-500 dark:text-gray-500 text-gray-400 cursor-not-allowed' 
                                    : 'bg-slate-700/50 dark:bg-white/5 dark:backdrop-blur-md dark:shadow-[4px_4px_8px_rgba(0,0,0,0.2),-2px_-2px_4px_rgba(255,255,255,0.03)] bg-gray-200 text-gray-300 dark:text-gray-300 text-gray-700 hover:bg-slate-700 dark:hover:bg-white/10 dark:hover:shadow-[6px_6px_12px_rgba(0,0,0,0.3),-3px_-3px_6px_rgba(255,255,255,0.05)] hover:bg-gray-300 hover:text-white dark:hover:text-white hover:text-gray-900'
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
                                                    ? 'bg-[#9546A7] text-white ring-2 ring-[#9546A7]/50' 
                                                    : 'bg-slate-700/50 dark:bg-white/5 dark:backdrop-blur-md dark:shadow-[4px_4px_8px_rgba(0,0,0,0.2),-2px_-2px_4px_rgba(255,255,255,0.03)] bg-gray-200 text-gray-400 dark:text-gray-400 text-gray-600 hover:bg-slate-700 dark:hover:bg-white/10 dark:hover:shadow-[6px_6px_12px_rgba(0,0,0,0.3),-3px_-3px_6px_rgba(255,255,255,0.05)] hover:bg-gray-300 hover:text-white dark:hover:text-white hover:text-gray-900'
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
                                    ? 'bg-slate-700/30 dark:bg-white/5 dark:backdrop-blur-md dark:shadow-[inset_2px_2px_4px_rgba(0,0,0,0.2),inset_-1px_-1px_2px_rgba(255,255,255,0.03)] bg-gray-200 text-gray-500 dark:text-gray-500 text-gray-400 cursor-not-allowed' 
                                    : 'bg-slate-700/50 dark:bg-white/5 dark:backdrop-blur-md dark:shadow-[4px_4px_8px_rgba(0,0,0,0.2),-2px_-2px_4px_rgba(255,255,255,0.03)] bg-gray-200 text-gray-300 dark:text-gray-300 text-gray-700 hover:bg-slate-700 dark:hover:bg-white/10 dark:hover:shadow-[6px_6px_12px_rgba(0,0,0,0.3),-3px_-3px_6px_rgba(255,255,255,0.05)] hover:bg-gray-300 hover:text-white dark:hover:text-white hover:text-gray-900'
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

// --- Theme Toggle Icon Components ---
const SunIcon = () => (
    <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z" />
    </svg>
);

const MoonIcon = () => (
    <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z" />
    </svg>
);

// --- Main App Component with MSAL Login Gate ---

export default function App() {
    const [activeView, setActiveView] = useState('dashboard');
    const [isDarkMode, setIsDarkMode] = useState(() => {
        // Check localStorage first, then default to light mode
        const saved = localStorage.getItem('theme');
        if (saved) {
            return saved === 'dark';
        }
        return false; // Default to light mode
    });

    const { instance, accounts } = useMsal();
    const isAuthenticated = useIsAuthenticated();

   // --- MSAL Login / Logout Handlers (Redirect Version) ---
const handleLogin = () => {
    instance.loginRedirect(loginRequest).catch(err => {
        console.error("MSAL login failed:", err);
    });
};

const handleLogout = () => {
    instance.logoutRedirect().catch(err => {
        console.error("MSAL logout failed:", err);
    });
};


    const userName = accounts[0]?.name || accounts[0]?.username;

    useEffect(() => {
        if (isDarkMode) {
            document.documentElement.classList.add('dark');
            localStorage.setItem('theme', 'dark');
        } else {
            document.documentElement.classList.remove('dark');
            localStorage.setItem('theme', 'light');
        }
    }, [isDarkMode]);

    const toggleTheme = () => {
        setIsDarkMode(!isDarkMode);
    };

    // --- Unauthenticated Login Screen ---
    if (!isAuthenticated) {
        return (
            <div className={`relative min-h-screen transition-colors duration-300 ${
                isDarkMode 
                    ? 'bg-black text-gray-200' 
                    : 'bg-gray-50 text-gray-900'
            }`} style={{ fontFamily: "'Inter', sans-serif" }}>
                {/* Animated Background */}
                <div className="fixed inset-0 overflow-hidden pointer-events-none">
                    <div className={`absolute -top-1/2 -right-1/2 w-full h-full rounded-full blur-3xl animate-pulse ${
                        isDarkMode 
                            ? 'bg-gradient-to-br from-[#9546A7]/10 via-cyan-500/5 to-transparent' 
                            : 'bg-gradient-to-br from-[#9546A7]/5 via-cyan-500/3 to-transparent'
                    }`}></div>
                    <div className={`absolute -bottom-1/2 -left-1/2 w-full h-full rounded-full blur-3xl animate-pulse ${
                        isDarkMode 
                            ? 'bg-gradient-to-tr from-blue-500/10 via-[#9546A7]/5 to-transparent' 
                            : 'bg-gradient-to-tr from-blue-500/5 via-[#9546A7]/3 to-transparent'
                    }`} style={{animationDelay: '1s'}}></div>
                </div>

                <div className="relative z-10 p-4 sm:p-6 lg:p-8">
                    <div className="max-w-6xl mx-auto">
                        {/* Top bar with theme toggle */}
                        <div className="flex justify-end mb-6">
                            <button
                                onClick={toggleTheme}
                                className="p-2.5 rounded-xl bg-white dark:bg-white/5 backdrop-blur-sm border border-gray-200 dark:border-white/10 text-gray-600 dark:text-gray-400 hover:text-[#9546A7] dark:hover:text-[#9546A7] hover:bg-gray-100 dark:hover:bg-white/10 transition-all duration-300 focus:outline-none focus:ring-2 focus:ring-[#9546A7]/50 dark:shadow-[4px_4px_8px_rgba(0,0,0,0.2),-2px_-2px_4px_rgba(255,255,255,0.03)]"
                                aria-label="Toggle theme"
                            >
                                {isDarkMode ? <SunIcon /> : <MoonIcon />}
                            </button>
                        </div>

                        <div className="grid gap-10 lg:grid-cols-2 items-center">
                            {/* Text / CTA */}
                            <div className="space-y-6">
                                <div className="inline-flex items-center space-x-3 px-3 py-1.5 rounded-full bg-white/70 dark:bg-white/5 border border-gray-200 dark:border-white/10 text-xs font-semibold text-gray-700 dark:text-gray-300 backdrop-blur">
                                    <span className="inline-flex h-2 w-2 rounded-full bg-emerald-400 animate-pulse"></span>
                                    <span>Secure NJ Courts access</span>
                                </div>
                                <div>
                                    <h1 className="text-3xl md:text-4xl font-bold text-gray-900 dark:bg-gradient-to-r dark:from-white dark:via-gray-100 dark:to-gray-300 dark:bg-clip-text dark:text-transparent mb-2">
                                        EZ Pass Fraud Detection Dashboard
                                    </h1>
                                    <p className="text-sm md:text-base text-gray-500 dark:text-gray-400 max-w-xl">
                                        Sign in with your NJ Courts Microsoft 365 account to review ML-powered fraud 
                                        alerts, visualize toll anomalies, and triage suspicious EZ Pass activity.
                                    </p>
                                </div>

                                <div className="space-y-3 text-sm text-gray-600 dark:text-gray-300">
                                    <div className="flex items-start space-x-3">
                                        <div className="mt-1 h-1.5 w-1.5 rounded-full bg-[#9546A7]" />
                                        <p><span className="font-semibold">Centralized view</span> of rule-based and ML-detected anomalies.</p>
                                    </div>
                                    <div className="flex items-start space-x-3">
                                        <div className="mt-1 h-1.5 w-1.5 rounded-full bg-blue-500" />
                                        <p><span className="font-semibold">Filter and sort</span> by plate, route, risk level, and more.</p>
                                    </div>
                                    <div className="flex items-start space-x-3">
                                        <div className="mt-1 h-1.5 w-1.5 rounded-full bg-emerald-500" />
                                        <p><span className="font-semibold">Track outcomes</span> with status updates on individual cases.</p>
                                    </div>
                                </div>

                                <div className="flex flex-wrap items-center gap-4">
                                    <button
                                        onClick={handleLogin}
                                        className="inline-flex items-center justify-center px-5 py-3 rounded-xl text-sm font-semibold text-white bg-gradient-to-r from-[#9546A7] to-[#7A3A8F] shadow-lg shadow-[#9546A7]/30 hover:shadow-xl hover:from-[#7A3A8F] hover:to-[#9546A7] transition-all focus:outline-none focus:ring-2 focus:ring-[#9546A7]/60"
                                    >
                                        <span className="mr-2">Sign in with Microsoft</span>
                                        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 8l4 4m0 0l-4 4m4-4H3" />
                                        </svg>
                                    </button>
                                    <p className="text-xs text-gray-500 dark:text-gray-400">
                                        Access restricted to authorized NJ Courts staff.
                                    </p>
                                </div>
                            </div>

                            {/* Right card / preview */}
                            <div className="hidden lg:block">
                                <div className="relative">
                                    <div className="absolute -inset-1 bg-gradient-to-tr from-[#9546A7]/40 via-indigo-500/40 to-cyan-500/40 blur-3xl opacity-60" />
                                    <div className="relative bg-white dark:bg-slate-900/80 border border-gray-200 dark:border-white/10 rounded-2xl p-5 shadow-2xl backdrop-blur-lg">
                                        <div className="flex items-center justify-between mb-4">
                                            <div className="flex items-center gap-3">
                                                <div className="bg-gradient-to-br from-[#9546A7] to-[#7A3A8F] p-2 rounded-xl text-white">
                                                    <ShieldIcon />
                                                </div>
                                                <div>
                                                    <p className="text-xs text-gray-500 dark:text-gray-400">Environment</p>
                                                    <p className="text-sm font-semibold text-gray-900 dark:text-gray-100">Fraud Analytics</p>
                                                </div>
                                            </div>
                                            <span className="px-3 py-1 rounded-full text-[10px] font-semibold bg-emerald-500/15 text-emerald-400 border border-emerald-500/30">
                                                SECURE • SSO
                                            </span>
                                        </div>
                                        <div className="space-y-3 text-xs text-gray-600 dark:text-gray-300">
                                            <div className="flex items-center justify-between">
                                                <span>Flagged Transactions (24h)</span>
                                                <span className="font-mono text-emerald-400">89</span>
                                            </div>
                                            <div className="flex items-center justify-between">
                                                <span>Critical Risk</span>
                                                <span className="font-mono text-red-400">12</span>
                                            </div>
                                            <div className="flex items-center justify-between">
                                                <span>ML Anomaly Coverage</span>
                                                <span className="font-mono text-sky-400">97.3%</span>
                                            </div>
                                        </div>
                                        <div className="mt-5 h-24 rounded-xl bg-gradient-to-r from-slate-900 via-slate-800 to-slate-900 border border-slate-700 flex items-center justify-center text-xs text-gray-400">
                                            Live dashboards unlock after sign-in.
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        );
    }

    // --- Authenticated Main App ---
    return (
        <div className={`relative min-h-screen transition-colors duration-300 ${
            isDarkMode 
                ? 'text-gray-200' 
                : 'bg-gray-50 text-gray-900'
        }`} style={{ fontFamily: "'Inter', sans-serif", backgroundColor: isDarkMode ? '#000000' : undefined }}>
            {/* Animated Background */}
            <div className="fixed inset-0 overflow-hidden pointer-events-none">
                <div className={`absolute -top-1/2 -right-1/2 w-full h-full rounded-full blur-3xl animate-pulse ${
                    isDarkMode 
                        ? 'bg-gradient-to-br from-[#9546A7]/10 via-cyan-500/5 to-transparent' 
                        : 'bg-gradient-to-br from-[#9546A7]/5 via-cyan-500/3 to-transparent'
                }`}></div>
                <div className={`absolute -bottom-1/2 -left-1/2 w-full h-full rounded-full blur-3xl animate-pulse ${
                    isDarkMode 
                        ? 'bg-gradient-to-tr from-blue-500/10 via-[#9546A7]/5 to-transparent' 
                        : 'bg-gradient-to-tr from-blue-500/5 via-[#9546A7]/3 to-transparent'
                }`} style={{animationDelay: '1s'}}></div>
            </div>

            <div className="relative z-10 p-4 sm:p-6 lg:p-8">
                <div className="max-w-[100rem] mx-auto">
                    {/* Header */}
                    <header className="mb-8">
                        <div className="bg-white dark:bg-white/5 backdrop-blur-xl border border-gray-200 dark:border-white/10 rounded-2xl p-6 shadow-2xl dark:shadow-[12px_12px_24px_rgba(0,0,0,0.4),-6px_-6px_12px_rgba(255,255,255,0.08)]">
                            <div className="flex flex-col lg:flex-row justify-between items-start lg:items-center gap-6">
                                {/* Logo and Title */}

                                <div className="flex items-center space-x-4">
                                    <div className="bg-gradient-to-br from-[#9546A7] to-[#7A3A8F] p-3 rounded-2xl shadow-lg text-white">
                                        <ShieldIcon />
                                    </div>
                                    <div>
                                        <h1 className="text-3xl md:text-4xl font-bold text-gray-900 dark:bg-gradient-to-r dark:from-white dark:via-gray-100 dark:to-gray-300 dark:bg-clip-text dark:text-transparent">
                                            EZ Pass Fraud Detection
                                        </h1>
                                        <p className="text-sm text-gray-400 dark:text-gray-400 text-gray-600 mt-1">New Jersey Courts</p>
                                    </div>
                                </div>

                                {/* Navigation / User / Theme */}
                                <div className="flex items-center gap-4">
                                    {/* Navigation Toggle */}
                                    <div className="flex items-center p-1.5 rounded-xl bg-white dark:bg-white/5 backdrop-blur-sm border border-gray-200 dark:border-white/10 shadow-inner dark:shadow-[inset_4px_4px_8px_rgba(0,0,0,0.3),inset_-2px_-2px_4px_rgba(255,255,255,0.05)]">
                                        <button 
                                            onClick={() => setActiveView('dashboard')} 
                                            className={`px-6 py-2.5 text-sm font-semibold rounded-lg focus:outline-none transition-all duration-300 ${
                                                activeView === 'dashboard' 
                                                    ? 'bg-gradient-to-r from-[#9546A7] to-[#7A3A8F] text-white' 
                                                    : 'text-gray-400 dark:text-gray-400 text-gray-600 hover:text-black dark:hover:text-white hover:text-gray-900'
                                            }`}
                                        >
                                            Dashboard
                                        </button>
                                        <button 
                                            onClick={() => setActiveView('data')} 
                                            className={`px-6 py-2.5 text-sm font-semibold rounded-lg focus:outline-none transition-all duration-300 ${
                                                activeView === 'data' 
                                                    ? 'bg-gradient-to-r from-[#9546A7] to-[#7A3A8F] text-white' 
                                                    : 'text-gray-400 dark:text-gray-400 text-gray-600 hover:text-blacks dark:hover:text-white hover:text-gray-900'
                                            }`}
                                        >
                                            Transactions
                                        </button>
                                        <button 
                                            onClick={() => setActiveView('charts')} 
                                            className={`px-6 py-2.5 text-sm font-semibold rounded-lg focus:outline-none transition-all duration-300 ${
                                                activeView === 'charts' 
                                                    ? 'bg-gradient-to-r from-[#9546A7] to-[#7A3A8F] text-white' 
                                                    : 'text-gray-400 dark:text-gray-400 text-gray-600 hover:text-black dark:hover:text-white hover:text-gray-900'
                                            }`}
                                        >
                                            Charts
                                        </button>
                                    </div>

                                    {/* User info + Sign out */}
                                    {userName && (
                                        <div className="hidden sm:flex flex-col items-end">
                                            <span className="text-xs text-gray-400 dark:text-gray-400">Signed in as</span>
                                            <span className="text-sm font-semibold text-gray-900 dark:text-gray-100 max-w-[200px] truncate">
                                                {userName}
                                            </span>
                                        </div>
                                    )}

                                    <button
                                        onClick={handleLogout}
                                        className="px-4 py-2.5 rounded-xl text-xs sm:text-sm font-semibold bg-white dark:bg-white/5 border border-gray-200 dark:border-white/10 text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-white/10 hover:text-[#9546A7] dark:hover:text-[#9546A7] transition-all focus:outline-none focus:ring-2 focus:ring-[#9546A7]/40"
                                    >
                                        Sign out
                                    </button>
                                    
                                    {/* Theme Toggle */}
                                    <button
                                        onClick={toggleTheme}
                                        className="p-2.5 rounded-xl bg-white dark:bg-white/5 backdrop-blur-sm border border-gray-200 dark:border-white/10 text-gray-600 dark:text-gray-400 hover:text-[#9546A7] dark:hover:text-[#9546A7] hover:bg-gray-100 dark:hover:bg-white/10 transition-all duration-300 focus:outline-none focus:ring-2 focus:ring-[#9546A7]/50 dark:shadow-[4px_4px_8px_rgba(0,0,0,0.2),-2px_-2px_4px_rgba(255,255,255,0.03)]"
                                        aria-label="Toggle theme"
                                    >
                                        {isDarkMode ? <SunIcon /> : <MoonIcon />}
                                    </button>
                                </div>
                            </div>
                        </div>
                    </header>

                    {/* Main Content */}
                    <main className="animate-fadeIn">
                        {activeView === 'dashboard' ? <DashboardView setActiveView={setActiveView} /> : 
                         activeView === 'charts' ? <ChartsView /> : 
                         <DataView />}
                    </main>
                </div>
            </div>
        </div>
    );
}
