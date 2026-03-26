'use client'

import { useEffect, useState } from 'react'
import { ShieldCheckIcon, ShieldExclamationIcon, ExclamationTriangleIcon, QuestionMarkCircleIcon } from '@heroicons/react/24/outline'

interface LicenseData {
    detected_license: string
    raw_license: string | null
    category: string
    commercial_use: boolean | null
    attribution_required: boolean | null
    share_alike: boolean | null
    description: string
    implications: string
    color: string
    confidence: string
}

interface SafetyBadge {
    status: 'safe' | 'caution' | 'restricted' | 'unknown'
    label: string
    color: string
}

interface CommercialUse {
    allowed: boolean | null
    conditions: string[]
    warning: string | null
}

interface LicenseAnalysis {
    license: LicenseData
    safety_badge: SafetyBadge
    commercial_use: CommercialUse
}

interface LicenseBadgeProps {
    datasetId: string
    initialData?: LicenseAnalysis
    compact?: boolean
    showDetails?: boolean
}

export default function LicenseBadge({
    datasetId,
    initialData,
    compact = false,
    showDetails = true
}: LicenseBadgeProps) {
    const [license, setLicense] = useState<LicenseAnalysis | null>(initialData || null)
    const [loading, setLoading] = useState(!initialData)
    const [error, setError] = useState<string | null>(null)
    const [expanded, setExpanded] = useState(false)

    useEffect(() => {
        if (!initialData && datasetId) {
            fetchLicenseAnalysis()
        }
    }, [datasetId, initialData])

    const fetchLicenseAnalysis = async () => {
        setLoading(true)
        try {
            const apiUrl = (process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001').replace(/\/api\/?$/, '')
            const response = await fetch(`${apiUrl}/api/datasets/${datasetId}/license`)
            const data = await response.json()

            if (data.status === 'success') {
                setLicense({
                    license: data.license,
                    safety_badge: data.safety_badge,
                    commercial_use: data.commercial_use
                })
            } else {
                setError('Failed to load license info')
            }
        } catch (err) {
            setError('Failed to load license info')
        } finally {
            setLoading(false)
        }
    }

    const getStatusIcon = (status: string) => {
        switch (status) {
            case 'safe':
                return <ShieldCheckIcon className="w-5 h-5" />
            case 'caution':
                return <ExclamationTriangleIcon className="w-5 h-5" />
            case 'restricted':
                return <ShieldExclamationIcon className="w-5 h-5" />
            default:
                return <QuestionMarkCircleIcon className="w-5 h-5" />
        }
    }

    const getStatusStyles = (color: string) => {
        switch (color) {
            case 'green':
                return {
                    badge: 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-400 border-emerald-200 dark:border-emerald-800',
                    icon: 'text-emerald-500',
                    bg: 'bg-emerald-50 dark:bg-emerald-900/20'
                }
            case 'yellow':
                return {
                    badge: 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400 border-yellow-200 dark:border-yellow-800',
                    icon: 'text-yellow-500',
                    bg: 'bg-yellow-50 dark:bg-yellow-900/20'
                }
            case 'orange':
                return {
                    badge: 'bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400 border-orange-200 dark:border-orange-800',
                    icon: 'text-orange-500',
                    bg: 'bg-orange-50 dark:bg-orange-900/20'
                }
            case 'red':
                return {
                    badge: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400 border-red-200 dark:border-red-800',
                    icon: 'text-red-500',
                    bg: 'bg-red-50 dark:bg-red-900/20'
                }
            default:
                return {
                    badge: 'bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-400 border-gray-200 dark:border-gray-700',
                    icon: 'text-gray-500',
                    bg: 'bg-gray-50 dark:bg-gray-800/50'
                }
        }
    }

    if (loading) {
        return (
            <div className="animate-pulse">
                <div className={`${compact ? 'h-6 w-24' : 'h-8 w-32'} bg-gray-300 dark:bg-gray-700 rounded-full`}></div>
            </div>
        )
    }

    if (error || !license) {
        return (
            <span className="inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400">
                <QuestionMarkCircleIcon className="w-4 h-4" />
                License Unknown
            </span>
        )
    }

    const { safety_badge, commercial_use } = license
    const styles = getStatusStyles(safety_badge.color)

    if (compact) {
        return (
            <span className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium border ${styles.badge}`}>
                <span className={styles.icon}>{getStatusIcon(safety_badge.status)}</span>
                {safety_badge.label}
            </span>
        )
    }

    return (
        <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700">
            {/* Header */}
            <div className="flex items-start justify-between mb-4">
                <div>
                    <h3 className="text-xl font-bold text-gray-900 dark:text-white mb-1">License Safety</h3>
                    <p className="text-sm text-gray-500 dark:text-gray-400">
                        {license.license.detected_license}
                    </p>
                </div>
                <span className={`inline-flex items-center gap-2 px-4 py-2 rounded-full font-medium border ${styles.badge}`}>
                    <span className={styles.icon}>{getStatusIcon(safety_badge.status)}</span>
                    {safety_badge.label}
                </span>
            </div>

            {/* Commercial Use Warning */}
            {commercial_use.warning && (
                <div className={`p-4 rounded-xl mb-4 ${styles.bg}`}>
                    <div className="flex items-start gap-3">
                        <span className={styles.icon}>
                            {commercial_use.allowed === false ? (
                                <ShieldExclamationIcon className="w-6 h-6" />
                            ) : (
                                <ExclamationTriangleIcon className="w-6 h-6" />
                            )}
                        </span>
                        <div>
                            <p className="font-medium text-gray-900 dark:text-white">
                                {commercial_use.allowed === true ? 'Commercial Use Allowed' :
                                    commercial_use.allowed === false ? 'Commercial Use Not Allowed' :
                                        'Commercial Use Unknown'}
                            </p>
                            <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
                                {commercial_use.warning}
                            </p>
                        </div>
                    </div>
                </div>
            )}

            {/* Conditions */}
            {commercial_use.conditions.length > 0 && (
                <div className="mb-4">
                    <h4 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">Conditions</h4>
                    <div className="flex flex-wrap gap-2">
                        {commercial_use.conditions.map((condition, index) => (
                            <span
                                key={index}
                                className="px-3 py-1 bg-gray-100 dark:bg-gray-800 text-gray-700 dark:text-gray-300 rounded-full text-sm"
                            >
                                {condition}
                            </span>
                        ))}
                    </div>
                </div>
            )}

            {/* Show Details Toggle */}
            {showDetails && (
                <>
                    <button
                        onClick={() => setExpanded(!expanded)}
                        className="text-primary-600 dark:text-primary-400 text-sm font-medium hover:underline"
                    >
                        {expanded ? 'Hide Details' : 'Can I use this dataset?'} →
                    </button>

                    {expanded && (
                        <div className="mt-4 pt-4 border-t border-gray-200 dark:border-gray-700">
                            <p className="text-gray-600 dark:text-gray-400 text-sm mb-4">
                                {license.license.implications}
                            </p>

                            <div className="grid grid-cols-3 gap-4">
                                <div className={`p-3 rounded-lg ${license.license.commercial_use ? 'bg-emerald-50 dark:bg-emerald-900/20' : 'bg-red-50 dark:bg-red-900/20'}`}>
                                    <p className="text-xs text-gray-500 dark:text-gray-400 mb-1">Commercial Use</p>
                                    <p className={`font-semibold ${license.license.commercial_use ? 'text-emerald-600' : 'text-red-600'}`}>
                                        {license.license.commercial_use === true ? '✓ Allowed' :
                                            license.license.commercial_use === false ? '✗ Not Allowed' : '? Unknown'}
                                    </p>
                                </div>
                                <div className={`p-3 rounded-lg ${license.license.attribution_required ? 'bg-yellow-50 dark:bg-yellow-900/20' : 'bg-emerald-50 dark:bg-emerald-900/20'}`}>
                                    <p className="text-xs text-gray-500 dark:text-gray-400 mb-1">Attribution</p>
                                    <p className={`font-semibold ${license.license.attribution_required ? 'text-yellow-600' : 'text-emerald-600'}`}>
                                        {license.license.attribution_required ? 'Required' : 'Not Required'}
                                    </p>
                                </div>
                                <div className={`p-3 rounded-lg ${license.license.share_alike ? 'bg-yellow-50 dark:bg-yellow-900/20' : 'bg-emerald-50 dark:bg-emerald-900/20'}`}>
                                    <p className="text-xs text-gray-500 dark:text-gray-400 mb-1">Share-Alike</p>
                                    <p className={`font-semibold ${license.license.share_alike ? 'text-yellow-600' : 'text-emerald-600'}`}>
                                        {license.license.share_alike ? 'Required' : 'Not Required'}
                                    </p>
                                </div>
                            </div>
                        </div>
                    )}
                </>
            )}
        </div>
    )
}
