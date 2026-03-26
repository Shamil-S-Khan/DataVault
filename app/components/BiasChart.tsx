'use client'

import { useEffect, useState } from 'react'
import { ShieldExclamationIcon, ExclamationTriangleIcon, InformationCircleIcon, CheckCircleIcon } from '@heroicons/react/24/outline'

interface BiasWarning {
    severity: 'low' | 'medium' | 'high'
    category: string
    title: string
    description: string
    recommendation: string
}

interface BiasAnalysis {
    risk_score: number
    risk_level: 'low' | 'medium' | 'high'
    warnings: BiasWarning[]
    warning_count: {
        high: number
        medium: number
        low: number
    }
    summary: string
    analyzed_aspects: string[]
}

interface BiasChartProps {
    datasetId: string
}

export default function BiasChart({ datasetId }: BiasChartProps) {
    const [analysis, setAnalysis] = useState<BiasAnalysis | null>(null)
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)
    const [expandedWarnings, setExpandedWarnings] = useState<Set<number>>(new Set())

    useEffect(() => {
        if (datasetId) {
            fetchBiasAnalysis()
        }
    }, [datasetId])

    const fetchBiasAnalysis = async () => {
        setLoading(true)
        try {
            const apiUrl = (process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001').replace(/\/api\/?$/, '')
            const response = await fetch(`${apiUrl}/api/datasets/${datasetId}/bias`)
            const data = await response.json()

            if (data.status === 'success') {
                setAnalysis(data)
            } else {
                setError('Failed to load bias analysis')
            }
        } catch (err) {
            setError('Failed to load bias analysis')
        } finally {
            setLoading(false)
        }
    }

    const getRiskStyles = (level: string) => {
        switch (level) {
            case 'low':
                return {
                    bg: 'bg-emerald-500',
                    text: 'text-emerald-500',
                    light: 'bg-emerald-100 dark:bg-emerald-900/30',
                    border: 'border-emerald-500'
                }
            case 'medium':
                return {
                    bg: 'bg-yellow-500',
                    text: 'text-yellow-500',
                    light: 'bg-yellow-100 dark:bg-yellow-900/30',
                    border: 'border-yellow-500'
                }
            case 'high':
                return {
                    bg: 'bg-red-500',
                    text: 'text-red-500',
                    light: 'bg-red-100 dark:bg-red-900/30',
                    border: 'border-red-500'
                }
            default:
                return {
                    bg: 'bg-gray-500',
                    text: 'text-gray-500',
                    light: 'bg-gray-100 dark:bg-gray-800',
                    border: 'border-gray-500'
                }
        }
    }

    const getSeverityIcon = (severity: string) => {
        switch (severity) {
            case 'high':
                return <ShieldExclamationIcon className="w-5 h-5 text-red-500" />
            case 'medium':
                return <ExclamationTriangleIcon className="w-5 h-5 text-yellow-500" />
            case 'low':
                return <InformationCircleIcon className="w-5 h-5 text-blue-500" />
            default:
                return <InformationCircleIcon className="w-5 h-5 text-gray-500" />
        }
    }

    const toggleWarning = (index: number) => {
        const newExpanded = new Set(expandedWarnings)
        if (newExpanded.has(index)) {
            newExpanded.delete(index)
        } else {
            newExpanded.add(index)
        }
        setExpandedWarnings(newExpanded)
    }

    if (loading) {
        return (
            <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700">
                <h3 className="text-xl font-bold text-gray-900 dark:text-white mb-4">Bias & Fairness</h3>
                <div className="space-y-4 animate-pulse">
                    <div className="h-24 bg-gray-200 dark:bg-gray-700 rounded-xl" />
                    <div className="h-16 bg-gray-200 dark:bg-gray-700 rounded-xl" />
                </div>
            </div>
        )
    }

    if (error || !analysis) {
        return (
            <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700">
                <h3 className="text-xl font-bold text-gray-900 dark:text-white mb-4">Bias & Fairness</h3>
                <div className="text-center py-8">
                    <ShieldExclamationIcon className="w-12 h-12 text-gray-400 mx-auto mb-3" />
                    <p className="text-gray-500 dark:text-gray-400">
                        {error || 'Unable to analyze bias'}
                    </p>
                </div>
            </div>
        )
    }

    const riskStyles = getRiskStyles(analysis.risk_level)

    return (
        <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700">
            {/* Header */}
            <div className="flex items-start justify-between mb-6">
                <div>
                    <h3 className="text-xl font-bold text-gray-900 dark:text-white">Bias & Fairness</h3>
                    <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                        Automated analysis for potential bias concerns
                    </p>
                </div>
            </div>

            {/* Risk Score Gauge */}
            <div className={`p-4 rounded-xl ${riskStyles.light} mb-6`}>
                <div className="flex items-center gap-4">
                    {/* Visual Gauge */}
                    <div className="relative w-20 h-20">
                        <svg className="w-20 h-20 -rotate-90" viewBox="0 0 100 100">
                            <circle
                                cx="50"
                                cy="50"
                                r="40"
                                fill="none"
                                stroke="currentColor"
                                strokeWidth="12"
                                className="text-gray-200 dark:text-gray-700"
                            />
                            <circle
                                cx="50"
                                cy="50"
                                r="40"
                                fill="none"
                                stroke="currentColor"
                                strokeWidth="12"
                                strokeDasharray={`${(analysis.risk_score / 100) * 251.2} 251.2`}
                                strokeLinecap="round"
                                className={riskStyles.text}
                            />
                        </svg>
                        <div className="absolute inset-0 flex items-center justify-center">
                            <span className={`text-xl font-bold ${riskStyles.text}`}>
                                {Math.round(analysis.risk_score)}
                            </span>
                        </div>
                    </div>

                    {/* Risk Info */}
                    <div className="flex-1">
                        <div className="flex items-center gap-2 mb-1">
                            <span className={`px-3 py-1 rounded-full text-sm font-bold uppercase ${riskStyles.light} ${riskStyles.text}`}>
                                {analysis.risk_level} Risk
                            </span>
                        </div>
                        <p className="text-sm text-gray-700 dark:text-gray-300">
                            {analysis.summary}
                        </p>
                    </div>
                </div>
            </div>

            {/* Warning Counts */}
            <div className="grid grid-cols-3 gap-4 mb-6">
                <div className="text-center p-3 bg-red-50 dark:bg-red-900/20 rounded-lg">
                    <p className="text-2xl font-bold text-red-500">{analysis.warning_count.high}</p>
                    <p className="text-xs text-gray-500 dark:text-gray-400">High</p>
                </div>
                <div className="text-center p-3 bg-yellow-50 dark:bg-yellow-900/20 rounded-lg">
                    <p className="text-2xl font-bold text-yellow-500">{analysis.warning_count.medium}</p>
                    <p className="text-xs text-gray-500 dark:text-gray-400">Medium</p>
                </div>
                <div className="text-center p-3 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
                    <p className="text-2xl font-bold text-blue-500">{analysis.warning_count.low}</p>
                    <p className="text-xs text-gray-500 dark:text-gray-400">Low</p>
                </div>
            </div>

            {/* Warnings List */}
            {analysis.warnings.length > 0 && (
                <div className="space-y-3">
                    <h4 className="text-sm font-semibold text-gray-700 dark:text-gray-300">Considerations</h4>
                    {analysis.warnings.map((warning, index) => (
                        <div
                            key={index}
                            className="bg-gray-50 dark:bg-gray-800/50 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden"
                        >
                            <button
                                onClick={() => toggleWarning(index)}
                                className="w-full p-3 flex items-start gap-3 text-left hover:bg-gray-100 dark:hover:bg-gray-700/50 transition-colors"
                            >
                                {getSeverityIcon(warning.severity)}
                                <div className="flex-1">
                                    <p className="font-medium text-gray-900 dark:text-white text-sm">
                                        {warning.title}
                                    </p>
                                    <p className="text-xs text-gray-500 dark:text-gray-400 capitalize">
                                        {warning.category.replace('_', ' ')}
                                    </p>
                                </div>
                                <span className={`px-2 py-0.5 rounded text-xs font-medium capitalize ${getRiskStyles(warning.severity).light} ${getRiskStyles(warning.severity).text}`}>
                                    {warning.severity}
                                </span>
                            </button>

                            {expandedWarnings.has(index) && (
                                <div className="px-3 pb-3 pt-1 border-t border-gray-200 dark:border-gray-700">
                                    <p className="text-sm text-gray-600 dark:text-gray-400 mb-2">
                                        {warning.description}
                                    </p>
                                    <div className="flex items-start gap-2 p-2 bg-primary-50 dark:bg-primary-900/20 rounded-lg">
                                        <CheckCircleIcon className="w-4 h-4 text-primary-500 mt-0.5 flex-shrink-0" />
                                        <p className="text-xs text-primary-700 dark:text-primary-400">
                                            <span className="font-semibold">Recommendation:</span> {warning.recommendation}
                                        </p>
                                    </div>
                                </div>
                            )}
                        </div>
                    ))}
                </div>
            )}
        </div>
    )
}
