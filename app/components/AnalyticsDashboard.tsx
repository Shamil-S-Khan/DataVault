'use client'

import { useEffect, useState } from 'react'
import { 
    ChartBarIcon, 
    ServerStackIcon, 
    TrophyIcon,
    ArrowTrendingUpIcon,
    CheckCircleIcon
} from '@heroicons/react/24/outline'
import Badge from './Badge'
import Link from 'next/link'

interface AnalyticsData {
    total_datasets: number
    platforms: Record<string, number>
    modalities: Record<string, number>
    top_domains: Record<string, number>
    quality_stats?: {
        avg_quality: number
        min_quality: number
        max_quality: number
    }
    size_stats?: {
        total_size_gb: number
        avg_size_gb: number
        max_size_gb: number
    }
    top_trending?: Array<{
        id: string
        name: string
        trend_score: number
        domain?: string
    }>
}

interface QualityAnalytics {
    label_distribution: Record<string, number>
    quality_by_platform: Array<{
        platform: string
        avg_quality: number
        count: number
    }>
    top_quality_datasets: Array<{
        id: string
        name: string
        quality_score: number
        quality_label: string
    }>
}

export default function AnalyticsDashboard() {
    const [analytics, setAnalytics] = useState<AnalyticsData | null>(null)
    const [qualityAnalytics, setQualityAnalytics] = useState<QualityAnalytics | null>(null)
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)

    useEffect(() => {
        fetchAnalytics()
    }, [])

    const fetchAnalytics = async () => {
        setLoading(true)
        setError(null)
        try {
            const apiUrl = (process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001').replace(/\/api\/?$/, '')
            
            // Fetch overview
            const overviewRes = await fetch(`${apiUrl}/api/analytics/overview`)
            if (!overviewRes.ok) throw new Error('Failed to fetch analytics')
            const overviewData = await overviewRes.json()
            setAnalytics(overviewData.overview)

            // Fetch quality analytics
            const qualityRes = await fetch(`${apiUrl}/api/analytics/quality-analytics`)
            if (qualityRes.ok) {
                const qualityData = await qualityRes.json()
                setQualityAnalytics(qualityData.quality_analytics)
            }
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Failed to load analytics')
        } finally {
            setLoading(false)
        }
    }

    if (loading) {
        return (
            <div className="space-y-6">
                {[...Array(4)].map((_, i) => (
                    <div key={i} className="animate-pulse bg-gray-200 dark:bg-gray-700 h-48 rounded-xl" />
                ))}
            </div>
        )
    }

    if (error || !analytics) {
        return (
            <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-xl p-6">
                <p className="text-red-800 dark:text-red-200">{error || 'Failed to load analytics'}</p>
            </div>
        )
    }

    return (
        <div className="space-y-6">
            {/* Header Stats */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <StatCard
                    icon={<ServerStackIcon className="w-6 h-6" />}
                    label="Total Datasets"
                    value={analytics.total_datasets.toLocaleString()}
                    color="blue"
                />
                <StatCard
                    icon={<ChartBarIcon className="w-6 h-6" />}
                    label="Platforms"
                    value={Object.keys(analytics.platforms).length.toString()}
                    color="green"
                />
                <StatCard
                    icon={<TrophyIcon className="w-6 h-6" />}
                    label="Modalities"
                    value={Object.keys(analytics.modalities).length.toString()}
                    color="purple"
                />
                <StatCard
                    icon={<CheckCircleIcon className="w-6 h-6" />}
                    label="Avg Quality"
                    value={analytics.quality_stats?.avg_quality ? 
                        `${(analytics.quality_stats.avg_quality * 100).toFixed(0)}%` : 'N/A'}
                    color="yellow"
                />
            </div>

            {/* Platform Distribution */}
            <div className="bg-white dark:bg-gray-800 rounded-xl p-6 shadow-sm border border-gray-200 dark:border-gray-700">
                <h3 className="text-lg font-bold text-gray-900 dark:text-white mb-4">
                    Platform Distribution
                </h3>
                <div className="space-y-3">
                    {Object.entries(analytics.platforms)
                        .sort(([, a], [, b]) => b - a)
                        .slice(0, 8)
                        .map(([platform, count]) => {
                            const percentage = (count / analytics.total_datasets) * 100
                            return (
                                <div key={platform}>
                                    <div className="flex items-center justify-between mb-1">
                                        <span className="text-sm font-medium text-gray-700 dark:text-gray-300 capitalize">
                                            {platform}
                                        </span>
                                        <span className="text-sm font-semibold text-gray-900 dark:text-white">
                                            {count.toLocaleString()} ({percentage.toFixed(1)}%)
                                        </span>
                                    </div>
                                    <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2">
                                        <div
                                            className="bg-primary-600 h-2 rounded-full transition-all duration-500"
                                            style={{ width: `${percentage}%` }}
                                        />
                                    </div>
                                </div>
                            )
                        })}
                </div>
            </div>

            {/* Modality & Quality Side by Side */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Modality Distribution */}
                <div className="bg-white dark:bg-gray-800 rounded-xl p-6 shadow-sm border border-gray-200 dark:border-gray-700">
                    <h3 className="text-lg font-bold text-gray-900 dark:text-white mb-4">
                        Modality Distribution
                    </h3>
                    <div className="grid grid-cols-2 gap-3">
                        {Object.entries(analytics.modalities)
                            .sort(([, a], [, b]) => b - a)
                            .slice(0, 8)
                            .map(([modality, count]) => (
                                <div key={modality} className="bg-gray-50 dark:bg-gray-700 rounded-lg p-3">
                                    <Badge variant="modality" size="sm">
                                        {modality}
                                    </Badge>
                                    <p className="text-2xl font-bold text-gray-900 dark:text-white mt-2">
                                        {count.toLocaleString()}
                                    </p>
                                    <p className="text-xs text-gray-500 dark:text-gray-400">
                                        {((count / analytics.total_datasets) * 100).toFixed(1)}%
                                    </p>
                                </div>
                            ))}
                    </div>
                </div>

                {/* Quality Distribution */}
                {qualityAnalytics?.label_distribution && (
                    <div className="bg-white dark:bg-gray-800 rounded-xl p-6 shadow-sm border border-gray-200 dark:border-gray-700">
                        <h3 className="text-lg font-bold text-gray-900 dark:text-white mb-4">
                            Quality Distribution
                        </h3>
                        <div className="space-y-3">
                            {Object.entries(qualityAnalytics.label_distribution)
                                .sort(([a], [b]) => {
                                    const order = ['Excellent', 'Good', 'Fair', 'Poor', 'Very Poor']
                                    return order.indexOf(a) - order.indexOf(b)
                                })
                                .map(([label, count]) => {
                                    const color = getQualityColor(label)
                                    return (
                                        <div key={label} className={`${color} rounded-lg p-3`}>
                                            <div className="flex items-center justify-between">
                                                <span className="font-semibold">{label}</span>
                                                <span className="font-bold">{count.toLocaleString()}</span>
                                            </div>
                                        </div>
                                    )
                                })}
                        </div>
                    </div>
                )}
            </div>

            {/* Top Trending Datasets */}
            {analytics.top_trending && analytics.top_trending.length > 0 && (
                <div className="bg-white dark:bg-gray-800 rounded-xl p-6 shadow-sm border border-gray-200 dark:border-gray-700">
                    <div className="flex items-center gap-2 mb-4">
                        <ArrowTrendingUpIcon className="w-6 h-6 text-primary-500" />
                        <h3 className="text-lg font-bold text-gray-900 dark:text-white">
                            Top Trending Datasets
                        </h3>
                    </div>
                    <div className="space-y-2">
                        {analytics.top_trending.slice(0, 10).map((dataset, idx) => (
                            <Link
                                key={dataset.id}
                                href={`/dataset/${dataset.id}`}
                                className="flex items-center gap-3 p-3 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors group"
                            >
                                <span className="text-2xl font-bold text-gray-400 dark:text-gray-600 w-8">
                                    #{idx + 1}
                                </span>
                                <div className="flex-1">
                                    <p className="font-semibold text-gray-900 dark:text-white group-hover:text-primary-600 dark:group-hover:text-primary-400">
                                        {dataset.name}
                                    </p>
                                    {dataset.domain && (
                                        <Badge variant="domain" size="xs">
                                            {dataset.domain}
                                        </Badge>
                                    )}
                                </div>
                                <span className="text-sm font-semibold text-primary-600 dark:text-primary-400">
                                    {dataset.trend_score?.toFixed(2)}
                                </span>
                            </Link>
                        ))}
                    </div>
                </div>
            )}
        </div>
    )
}

function StatCard({ icon, label, value, color }: { 
    icon: React.ReactNode
    label: string
    value: string
    color: 'blue' | 'green' | 'purple' | 'yellow'
}) {
    const colorClasses = {
        blue: 'bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400',
        green: 'bg-green-100 dark:bg-green-900/30 text-green-600 dark:text-green-400',
        purple: 'bg-purple-100 dark:bg-purple-900/30 text-purple-600 dark:text-purple-400',
        yellow: 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-600 dark:text-yellow-400'
    }

    return (
        <div className="bg-white dark:bg-gray-800 rounded-xl p-6 shadow-sm border border-gray-200 dark:border-gray-700">
            <div className={`w-12 h-12 rounded-lg ${colorClasses[color]} flex items-center justify-center mb-3`}>
                {icon}
            </div>
            <p className="text-sm text-gray-600 dark:text-gray-400 mb-1">{label}</p>
            <p className="text-3xl font-bold text-gray-900 dark:text-white">{value}</p>
        </div>
    )
}

function getQualityColor(label: string): string {
    switch (label) {
        case 'Excellent':
            return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300'
        case 'Good':
            return 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300'
        case 'Fair':
            return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300'
        case 'Poor':
            return 'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-300'
        case 'Very Poor':
            return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300'
        default:
            return 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300'
    }
}
