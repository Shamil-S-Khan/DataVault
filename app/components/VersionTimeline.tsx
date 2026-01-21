'use client'

import { useEffect, useState } from 'react'
import { ClockIcon, ArrowTrendingUpIcon, ArrowTrendingDownIcon, ExclamationTriangleIcon, CheckCircleIcon } from '@heroicons/react/24/outline'

interface Version {
    version: string
    timestamp: string
    samples: number | null
    downloads: number | null
    likes: number | null
    file_size: number | null
    is_current: boolean
}

interface DriftAlert {
    field: string
    previous_value: any
    current_value: any
    change_percent: number | null
    severity: 'low' | 'medium' | 'high'
    description: string
}

interface VersionTimelineProps {
    datasetId: string
}

export default function VersionTimeline({ datasetId }: VersionTimelineProps) {
    const [versions, setVersions] = useState<Version[]>([])
    const [driftAlerts, setDriftAlerts] = useState<DriftAlert[]>([])
    const [driftScore, setDriftScore] = useState<number>(0)
    const [driftLevel, setDriftLevel] = useState<string>('stable')
    const [loading, setLoading] = useState(true)
    const [expanded, setExpanded] = useState(false)

    useEffect(() => {
        if (datasetId) {
            fetchData()
        }
    }, [datasetId])

    const fetchData = async () => {
        setLoading(true)
        try {
            const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

            const [versionsRes, driftRes] = await Promise.all([
                fetch(`${apiUrl}/api/datasets/${datasetId}/versions`),
                fetch(`${apiUrl}/api/datasets/${datasetId}/drift`)
            ])

            const versionsData = await versionsRes.json()
            const driftData = await driftRes.json()

            if (versionsData.status === 'success') {
                setVersions(versionsData.versions || [])
            }

            if (driftData.status === 'success') {
                setDriftAlerts(driftData.alerts || [])
                setDriftScore(driftData.drift_score || 0)
                setDriftLevel(driftData.drift_level || 'stable')
            }
        } catch (err) {
            console.error('Failed to load version data:', err)
        } finally {
            setLoading(false)
        }
    }

    const getDriftStyles = (level: string) => {
        switch (level) {
            case 'stable':
                return { bg: 'bg-emerald-500', text: 'text-emerald-500', light: 'bg-emerald-100 dark:bg-emerald-900/30' }
            case 'low':
                return { bg: 'bg-blue-500', text: 'text-blue-500', light: 'bg-blue-100 dark:bg-blue-900/30' }
            case 'medium':
                return { bg: 'bg-yellow-500', text: 'text-yellow-500', light: 'bg-yellow-100 dark:bg-yellow-900/30' }
            case 'high':
                return { bg: 'bg-red-500', text: 'text-red-500', light: 'bg-red-100 dark:bg-red-900/30' }
            default:
                return { bg: 'bg-gray-500', text: 'text-gray-500', light: 'bg-gray-100 dark:bg-gray-800' }
        }
    }

    const formatDate = (isoString: string) => {
        try {
            return new Date(isoString).toLocaleDateString('en-US', {
                year: 'numeric',
                month: 'short',
                day: 'numeric'
            })
        } catch {
            return isoString
        }
    }

    const formatNumber = (num: number | null) => {
        if (num === null || num === undefined) return '-'
        if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`
        if (num >= 1000) return `${(num / 1000).toFixed(1)}K`
        return num.toString()
    }

    if (loading) {
        return (
            <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700">
                <h3 className="text-xl font-bold text-gray-900 dark:text-white mb-4">Version History</h3>
                <div className="animate-pulse space-y-4">
                    <div className="h-16 bg-gray-200 dark:bg-gray-700 rounded-xl" />
                    <div className="h-24 bg-gray-200 dark:bg-gray-700 rounded-xl" />
                </div>
            </div>
        )
    }

    const styles = getDriftStyles(driftLevel)

    return (
        <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700">
            {/* Header */}
            <div className="flex items-start justify-between mb-6">
                <div>
                    <h3 className="text-xl font-bold text-gray-900 dark:text-white">Version History</h3>
                    <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                        Track changes and detect drift over time
                    </p>
                </div>
                <div className={`px-3 py-1 rounded-full text-sm font-bold uppercase ${styles.light} ${styles.text}`}>
                    {driftLevel === 'stable' ? '✓ Stable' : `${driftLevel} drift`}
                </div>
            </div>

            {/* Drift Score */}
            {driftScore > 0 && (
                <div className={`p-4 rounded-xl ${styles.light} mb-6`}>
                    <div className="flex items-center gap-3">
                        {driftLevel === 'stable' ? (
                            <CheckCircleIcon className={`w-8 h-8 ${styles.text}`} />
                        ) : (
                            <ExclamationTriangleIcon className={`w-8 h-8 ${styles.text}`} />
                        )}
                        <div>
                            <p className="font-semibold text-gray-900 dark:text-white">
                                Drift Score: {Math.round(driftScore)}/100
                            </p>
                            <p className="text-sm text-gray-600 dark:text-gray-400">
                                {driftAlerts.length} alert{driftAlerts.length !== 1 ? 's' : ''} detected
                            </p>
                        </div>
                    </div>
                </div>
            )}

            {/* Drift Alerts */}
            {driftAlerts.length > 0 && (
                <div className="space-y-2 mb-6">
                    {driftAlerts.map((alert, idx) => (
                        <div
                            key={idx}
                            className={`p-3 rounded-lg border ${alert.severity === 'high' ? 'border-red-300 dark:border-red-800 bg-red-50 dark:bg-red-900/20' :
                                    alert.severity === 'medium' ? 'border-yellow-300 dark:border-yellow-800 bg-yellow-50 dark:bg-yellow-900/20' :
                                        'border-blue-300 dark:border-blue-800 bg-blue-50 dark:bg-blue-900/20'
                                }`}
                        >
                            <p className="text-sm font-medium text-gray-900 dark:text-white">
                                {alert.description}
                            </p>
                        </div>
                    ))}
                </div>
            )}

            {/* Version Timeline */}
            <div className="relative">
                <button
                    onClick={() => setExpanded(!expanded)}
                    className="w-full text-left p-3 rounded-lg bg-gray-50 dark:bg-gray-800/50 hover:bg-gray-100 dark:hover:bg-gray-700/50 transition-colors"
                >
                    <div className="flex items-center justify-between">
                        <span className="font-medium text-gray-900 dark:text-white flex items-center gap-2">
                            <ClockIcon className="w-5 h-5" />
                            {versions.length} Version{versions.length !== 1 ? 's' : ''} Tracked
                        </span>
                        <span className="text-sm text-gray-500">
                            {expanded ? '▲ Collapse' : '▼ Expand'}
                        </span>
                    </div>
                </button>

                {expanded && (
                    <div className="mt-4 relative pl-4 border-l-2 border-primary-300 dark:border-primary-700 space-y-4">
                        {versions.map((version, idx) => (
                            <div key={idx} className="relative">
                                {/* Timeline dot */}
                                <div className={`absolute -left-[1.35rem] w-4 h-4 rounded-full ${version.is_current ? 'bg-primary-500' : 'bg-gray-400'
                                    }`} />

                                <div className={`p-4 rounded-lg ${version.is_current
                                        ? 'bg-primary-50 dark:bg-primary-900/20 border border-primary-200 dark:border-primary-800'
                                        : 'bg-gray-50 dark:bg-gray-800/50'
                                    }`}>
                                    <div className="flex items-center justify-between mb-2">
                                        <span className="font-semibold text-gray-900 dark:text-white">
                                            {version.version}
                                            {version.is_current && (
                                                <span className="ml-2 text-xs px-2 py-0.5 bg-primary-500 text-white rounded-full">
                                                    Current
                                                </span>
                                            )}
                                        </span>
                                        <span className="text-sm text-gray-500">
                                            {formatDate(version.timestamp)}
                                        </span>
                                    </div>

                                    <div className="grid grid-cols-3 gap-4 text-sm">
                                        <div>
                                            <span className="text-gray-500">Downloads</span>
                                            <p className="font-medium text-gray-900 dark:text-white">
                                                {formatNumber(version.downloads)}
                                            </p>
                                        </div>
                                        <div>
                                            <span className="text-gray-500">Likes</span>
                                            <p className="font-medium text-gray-900 dark:text-white">
                                                {formatNumber(version.likes)}
                                            </p>
                                        </div>
                                        <div>
                                            <span className="text-gray-500">Samples</span>
                                            <p className="font-medium text-gray-900 dark:text-white">
                                                {formatNumber(version.samples)}
                                            </p>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        ))}
                    </div>
                )}
            </div>
        </div>
    )
}
