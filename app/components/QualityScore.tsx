'use client'

import { useEffect, useState } from 'react'
import { 
    CheckCircleIcon, 
    ExclamationTriangleIcon, 
    InformationCircleIcon,
    ChartBarIcon
} from '@heroicons/react/24/outline'

interface QualityData {
    overall: number
    label: string
    completeness?: number
    documentation?: number
    metadata_richness?: number
    community_validation?: number
}

interface QualityScoreProps {
    datasetId: string
    compact?: boolean
    showBreakdown?: boolean
}

export default function QualityScore({ datasetId, compact = false, showBreakdown = true }: QualityScoreProps) {
    const [quality, setQuality] = useState<QualityData | null>(null)
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)
    const [showDetails, setShowDetails] = useState(false)

    useEffect(() => {
        if (datasetId) {
            fetchQuality()
        }
    }, [datasetId])

    const fetchQuality = async () => {
        setLoading(true)
        setError(null)
        try {
            const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
            const url = `${apiUrl}/api/datasets/${datasetId}/quality?detailed=${showBreakdown}`
            console.log('Fetching quality from:', url)
            const response = await fetch(url)
            console.log('Response status:', response.status)
            if (!response.ok) throw new Error('Failed to fetch quality score')
            const data = await response.json()
            console.log('Quality data:', data)
            setQuality(data.quality)
        } catch (err) {
            console.error('Quality fetch error:', err)
            setError(err instanceof Error ? err.message : 'Unknown error')
        } finally {
            setLoading(false)
        }
    }

    const getQualityColor = (score: number) => {
        if (score >= 0.8) return 'text-green-600 dark:text-green-400'
        if (score >= 0.6) return 'text-blue-600 dark:text-blue-400'
        if (score >= 0.4) return 'text-yellow-600 dark:text-yellow-400'
        if (score >= 0.2) return 'text-orange-600 dark:text-orange-400'
        return 'text-red-600 dark:text-red-400'
    }

    const getQualityBgColor = (score: number) => {
        if (score >= 0.8) return 'bg-green-100 dark:bg-green-900/30 border-green-200 dark:border-green-800'
        if (score >= 0.6) return 'bg-blue-100 dark:bg-blue-900/30 border-blue-200 dark:border-blue-800'
        if (score >= 0.4) return 'bg-yellow-100 dark:bg-yellow-900/30 border-yellow-200 dark:border-yellow-800'
        if (score >= 0.2) return 'bg-orange-100 dark:bg-orange-900/30 border-orange-200 dark:border-orange-800'
        return 'bg-red-100 dark:bg-red-900/30 border-red-200 dark:border-red-800'
    }

    const getQualityIcon = (score: number) => {
        if (score >= 0.6) return <CheckCircleIcon className="w-5 h-5" />
        if (score >= 0.4) return <InformationCircleIcon className="w-5 h-5" />
        return <ExclamationTriangleIcon className="w-5 h-5" />
    }

    if (loading) {
        return (
            <div className={`animate-pulse ${compact ? 'h-8' : 'h-32'} bg-gray-200 dark:bg-gray-700 rounded-lg`} />
        )
    }

    if (error || !quality) {
        return (
            <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-xl p-4">
                <p className="text-red-600 dark:text-red-400 text-sm">
                    Failed to load quality score: {error || 'No data'}
                </p>
            </div>
        )
    }

    if (compact) {
        return (
            <div className={`inline-flex items-center gap-2 px-3 py-1.5 rounded-full border ${getQualityBgColor(quality.overall)}`}>
                <span className={`font-semibold text-sm ${getQualityColor(quality.overall)}`}>
                    {quality.label}
                </span>
                <span className="text-xs text-gray-600 dark:text-gray-400">
                    {(quality.overall * 100).toFixed(0)}%
                </span>
            </div>
        )
    }

    return (
        <div className="bg-white dark:bg-gray-800 rounded-xl p-6 shadow-sm border border-gray-200 dark:border-gray-700">
            <div className="flex items-start justify-between mb-4">
                <div>
                    <div className="flex items-center gap-2 mb-1">
                        <ChartBarIcon className="w-5 h-5 text-primary-500" />
                        <h3 className="text-lg font-bold text-gray-900 dark:text-white">
                            Quality Assessment
                        </h3>
                    </div>
                    <p className="text-sm text-gray-600 dark:text-gray-400">
                        Multi-factor quality evaluation
                    </p>
                </div>
            </div>

            {/* Overall Score */}
            <div className={`flex items-center justify-between p-4 rounded-lg border-2 mb-4 ${getQualityBgColor(quality.overall)}`}>
                <div className="flex items-center gap-3">
                    <div className={getQualityColor(quality.overall)}>
                        {getQualityIcon(quality.overall)}
                    </div>
                    <div>
                        <p className="text-sm text-gray-600 dark:text-gray-400 font-medium">Overall Quality</p>
                        <p className={`text-2xl font-bold ${getQualityColor(quality.overall)}`}>
                            {quality.label}
                        </p>
                    </div>
                </div>
                <div className={`text-3xl font-bold ${getQualityColor(quality.overall)}`}>
                    {(quality.overall * 100).toFixed(0)}%
                </div>
            </div>

            {/* Breakdown */}
            {showBreakdown && quality.completeness !== undefined && (
                <div className="space-y-3">
                    <button
                        onClick={() => setShowDetails(!showDetails)}
                        className="text-sm text-primary-600 dark:text-primary-400 hover:underline font-medium"
                    >
                        {showDetails ? '▼ Hide Details' : '▶ Show Breakdown'}
                    </button>

                    {showDetails && (
                        <div className="space-y-3 animate-fade-in">
                            {/* Completeness */}
                            <QualityBar
                                label="Completeness"
                                score={quality.completeness}
                                description="Has description, license, size, metadata"
                            />

                            {/* Documentation */}
                            <QualityBar
                                label="Documentation"
                                score={quality.documentation}
                                description="Description quality and detail"
                            />

                            {/* Metadata Richness */}
                            <QualityBar
                                label="Metadata Richness"
                                score={quality.metadata_richness}
                                description="Number and value of metadata fields"
                            />

                            {/* Community Validation */}
                            <QualityBar
                                label="Community Validation"
                                score={quality.community_validation}
                                description="Downloads, likes, usage indicators"
                            />
                        </div>
                    )}
                </div>
            )}
        </div>
    )
}

// Quality Bar Component
function QualityBar({ label, score, description }: { label: string; score?: number; description: string }) {
    if (score === undefined) return null

    const percentage = Math.round(score * 100)
    const getBarColor = (s: number) => {
        if (s >= 0.8) return 'bg-green-500'
        if (s >= 0.6) return 'bg-blue-500'
        if (s >= 0.4) return 'bg-yellow-500'
        return 'bg-red-500'
    }

    return (
        <div className="space-y-1">
            <div className="flex items-center justify-between">
                <span className="text-sm font-medium text-gray-700 dark:text-gray-300">{label}</span>
                <span className="text-sm font-semibold text-gray-900 dark:text-white">{percentage}%</span>
            </div>
            <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2 overflow-hidden">
                <div
                    className={`h-full ${getBarColor(score)} transition-all duration-500 rounded-full`}
                    style={{ width: `${percentage}%` }}
                />
            </div>
            <p className="text-xs text-gray-500 dark:text-gray-400">{description}</p>
        </div>
    )
}
