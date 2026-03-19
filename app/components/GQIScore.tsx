'use client'

import { useEffect, useState } from 'react'
import {
    ShieldCheckIcon,
    CheckCircleIcon,
    ExclamationTriangleIcon,
    InformationCircleIcon,
    SparklesIcon,
    BeakerIcon,
    AcademicCapIcon,
    CpuChipIcon,
    ChartPieIcon,
} from '@heroicons/react/24/outline'

/* ------------------------------------------------------------------ */
/* Types                                                               */
/* ------------------------------------------------------------------ */

interface GQIBreakdown {
    structural_clarity: number
    representational_entropy: number
    academic_authority: number
    operational_fitness: number
}

interface LabelTrust {
    high_signals: string[]
    low_signals: string[]
}

interface SyntheticResilience {
    verdict: string
    score: number
    risk_level: string
}

interface GQIData {
    score: number
    grade: string
    raw_score: number
    label_reliability_multiplier: number
    breakdown: GQIBreakdown
    label_trust: LabelTrust
    synthetic_resilience: SyntheticResilience
    utility_note: string | null
    explanation: string
    calculated_at: string
}

interface GQIScoreProps {
    datasetId: string
    compact?: boolean
}

/* ------------------------------------------------------------------ */
/* Main Component                                                      */
/* ------------------------------------------------------------------ */

export default function GQIScore({ datasetId, compact = false }: GQIScoreProps) {
    const [gqi, setGqi] = useState<GQIData | null>(null)
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)
    const [showBreakdown, setShowBreakdown] = useState(false)

    useEffect(() => {
        if (datasetId) fetchGQI()
    }, [datasetId])

    const fetchGQI = async () => {
        setLoading(true)
        setError(null)
        try {
            const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
            const res = await fetch(`${apiUrl}/api/datasets/${datasetId}/gqi`)
            if (!res.ok) throw new Error('Failed to fetch GQI')
            const data = await res.json()
            setGqi(data.gqi)
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Unknown error')
        } finally {
            setLoading(false)
        }
    }

    /* ---------- colour helpers ---------- */

    const gradeColor = (grade: string) => {
        if (grade === 'A+' || grade === 'A') return { text: 'text-emerald-500', bg: 'bg-emerald-500/10', ring: 'ring-emerald-500/30', bar: 'bg-emerald-500' }
        if (grade.startsWith('B')) return { text: 'text-blue-500', bg: 'bg-blue-500/10', ring: 'ring-blue-500/30', bar: 'bg-blue-500' }
        if (grade.startsWith('C')) return { text: 'text-yellow-500', bg: 'bg-yellow-500/10', ring: 'ring-yellow-500/30', bar: 'bg-yellow-500' }
        if (grade === 'D') return { text: 'text-orange-500', bg: 'bg-orange-500/10', ring: 'ring-orange-500/30', bar: 'bg-orange-500' }
        return { text: 'text-red-500', bg: 'bg-red-500/10', ring: 'ring-red-500/30', bar: 'bg-red-500' }
    }

    const barColor = (v: number) => {
        if (v >= 0.8) return 'bg-emerald-500'
        if (v >= 0.6) return 'bg-blue-500'
        if (v >= 0.4) return 'bg-yellow-500'
        return 'bg-red-500'
    }

    /* ---------- loading / error states ---------- */

    if (loading) {
        return (
            <div className={`animate-pulse ${compact ? 'h-14' : 'h-48'} bg-gray-200 dark:bg-gray-700 rounded-2xl`} />
        )
    }

    if (error || !gqi) {
        return (
            <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-xl p-4">
                <p className="text-red-600 dark:text-red-400 text-sm">
                    Failed to load GQI: {error || 'No data'}
                </p>
            </div>
        )
    }

    const colors = gradeColor(gqi.grade)
    const pct = Math.round(gqi.score * 100)
    const circumference = 2 * Math.PI * 45
    const progress = gqi.score * circumference

    /* ---------- compact variant ---------- */

    if (compact) {
        return (
            <div className="flex items-center gap-3">
                <div className="relative w-12 h-12">
                    <svg className="w-12 h-12 -rotate-90" viewBox="0 0 100 100">
                        <circle cx="50" cy="50" r="45" fill="none" stroke="currentColor" strokeWidth="8"
                            className="text-gray-200 dark:text-gray-700" />
                        <circle cx="50" cy="50" r="45" fill="none" stroke="currentColor" strokeWidth="8"
                            strokeDasharray={circumference} strokeDashoffset={circumference - progress}
                            strokeLinecap="round" className={`${colors.text} transition-all duration-1000`} />
                    </svg>
                    <div className="absolute inset-0 flex items-center justify-center">
                        <span className={`text-xs font-bold ${colors.text}`}>{pct}</span>
                    </div>
                </div>
                <div>
                    <span className={`text-xs font-semibold px-2 py-0.5 rounded-full ${colors.bg} ${colors.text}`}>
                        {gqi.grade}
                    </span>
                    <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">GQI Score</p>
                </div>
            </div>
        )
    }

    /* ---------- full variant ---------- */

    const dimensions: { key: keyof GQIBreakdown; label: string; icon: JSX.Element; desc: string }[] = [
        { key: 'structural_clarity', label: 'Structural Clarity', icon: <CpuChipIcon className="w-4 h-4" />, desc: 'How well fields are typed & mapped' },
        { key: 'representational_entropy', label: 'Class Balance', icon: <ChartPieIcon className="w-4 h-4" />, desc: 'Shannon entropy of label distribution' },
        { key: 'academic_authority', label: 'Academic Authority', icon: <AcademicCapIcon className="w-4 h-4" />, desc: 'Citations, downloads & benchmark signals' },
        { key: 'operational_fitness', label: 'Operational Fitness', icon: <BeakerIcon className="w-4 h-4" />, desc: 'Size, license, freshness & community' },
    ]

    const lrColor = gqi.label_reliability_multiplier >= 1.0
        ? 'text-emerald-600 dark:text-emerald-400 bg-emerald-50 dark:bg-emerald-900/20 border-emerald-200 dark:border-emerald-800'
        : 'text-orange-600 dark:text-orange-400 bg-orange-50 dark:bg-orange-900/20 border-orange-200 dark:border-orange-800'

    return (
        <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700">
            {/* Header row */}
            <div className="flex items-start gap-6">
                {/* Ring gauge */}
                <div className="relative shrink-0">
                    <div className={`w-28 h-28 rounded-full ${colors.ring} ring-8`}>
                        <svg className="w-28 h-28 -rotate-90" viewBox="0 0 100 100">
                            <circle cx="50" cy="50" r="45" fill="none" stroke="currentColor" strokeWidth="10"
                                className="text-gray-200 dark:text-gray-700" />
                            <circle cx="50" cy="50" r="45" fill="none" stroke="currentColor" strokeWidth="10"
                                strokeDasharray={circumference} strokeDashoffset={circumference - progress}
                                strokeLinecap="round" className={`${colors.text} transition-all duration-1000`} />
                        </svg>
                        <div className="absolute inset-0 flex flex-col items-center justify-center">
                            <span className={`text-3xl font-bold ${colors.text}`}>{pct}</span>
                            <span className="text-xs text-gray-500 dark:text-gray-400">/100</span>
                        </div>
                    </div>
                </div>

                {/* Text */}
                <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-3 mb-2 flex-wrap">
                        <h3 className="text-xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                            <ShieldCheckIcon className="w-6 h-6 text-primary-500" />
                            Global Quality Index
                        </h3>
                        <span className={`px-3 py-1 rounded-full text-sm font-bold ${colors.bg} ${colors.text}`}>
                            Grade: {gqi.grade}
                        </span>
                    </div>

                    <p className="text-gray-600 dark:text-gray-400 text-sm mb-3 leading-relaxed">
                        {gqi.explanation}
                    </p>

                    {/* Label Reliability badge */}
                    <div className={`inline-flex items-center gap-2 px-3 py-1.5 rounded-lg border text-xs font-medium ${lrColor}`}>
                        {gqi.label_reliability_multiplier >= 1.0
                            ? <CheckCircleIcon className="w-4 h-4" />
                            : <ExclamationTriangleIcon className="w-4 h-4" />}
                        Label Reliability: {gqi.label_reliability_multiplier.toFixed(2)}×
                    </div>

                    <button
                        onClick={() => setShowBreakdown(!showBreakdown)}
                        className="block mt-3 text-primary-600 dark:text-primary-400 text-sm font-medium hover:underline"
                    >
                        {showBreakdown ? 'Hide Breakdown' : 'Show Breakdown'} →
                    </button>
                </div>
            </div>

            {/* Breakdown panel */}
            {showBreakdown && (
                <div className="mt-6 pt-6 border-t border-gray-200 dark:border-gray-700 space-y-5 animate-fade-in">
                    <h4 className="text-sm font-semibold text-gray-700 dark:text-gray-300">Score Breakdown</h4>

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        {dimensions.map(dim => {
                            const val = gqi.breakdown[dim.key]
                            const valPct = Math.round(val * 100)
                            return (
                                <div key={dim.key} className="bg-gray-50 dark:bg-gray-800/50 rounded-xl p-4">
                                    <div className="flex items-center justify-between mb-2">
                                        <span className="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-300">
                                            {dim.icon} {dim.label}
                                        </span>
                                        <span className={`text-sm font-bold ${barColor(val).replace('bg-', 'text-')}`}>
                                            {valPct}%
                                        </span>
                                    </div>
                                    <div className="w-full h-2 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
                                        <div
                                            className={`h-full ${barColor(val)} transition-all duration-700 rounded-full`}
                                            style={{ width: `${valPct}%` }}
                                        />
                                    </div>
                                    <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">{dim.desc}</p>
                                </div>
                            )
                        })}
                    </div>

                    {/* Label Trust details */}
                    {(gqi.label_trust.high_signals.length > 0 || gqi.label_trust.low_signals.length > 0) && (
                        <div className="bg-gray-50 dark:bg-gray-800/50 rounded-xl p-4">
                            <h5 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">Label Trust Signals</h5>
                            <div className="flex flex-wrap gap-2">
                                {gqi.label_trust.high_signals.map(s => (
                                    <span key={s} className="text-xs px-2 py-1 rounded-full bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-300 font-medium">
                                        ✓ {s}
                                    </span>
                                ))}
                                {gqi.label_trust.low_signals.map(s => (
                                    <span key={s} className="text-xs px-2 py-1 rounded-full bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300 font-medium">
                                        ⚠ {s}
                                    </span>
                                ))}
                            </div>
                        </div>
                    )}

                    {/* Synthetic Resilience banner */}
                    <div className="bg-purple-50 dark:bg-purple-900/20 border border-purple-200 dark:border-purple-800 rounded-xl p-4">
                        <div className="flex items-center gap-2 mb-1">
                            <SparklesIcon className="w-4 h-4 text-purple-600 dark:text-purple-400" />
                            <h5 className="text-sm font-semibold text-purple-700 dark:text-purple-300">Synthetic Resilience</h5>
                        </div>
                        <p className="text-sm text-purple-600 dark:text-purple-400">
                            {gqi.synthetic_resilience.verdict} — Score: {gqi.synthetic_resilience.score}/100
                        </p>
                    </div>

                    {/* Utility note */}
                    {gqi.utility_note && (
                        <div className="bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded-xl p-4 flex items-start gap-2">
                            <InformationCircleIcon className="w-5 h-5 text-amber-600 dark:text-amber-400 shrink-0 mt-0.5" />
                            <p className="text-sm text-amber-700 dark:text-amber-300">{gqi.utility_note}</p>
                        </div>
                    )}
                </div>
            )}
        </div>
    )
}
