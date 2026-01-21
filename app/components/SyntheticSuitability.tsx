'use client'

import { useEffect, useState } from 'react'
import {
    BeakerIcon,
    ShieldExclamationIcon,
    CheckCircleIcon,
    ExclamationTriangleIcon,
    InformationCircleIcon,
    ChevronDownIcon,
    ChevronUpIcon,
    ScaleIcon
} from '@heroicons/react/24/outline'

interface BenefitFactor {
    name: string
    points: number
    explanation: string
    icon: string
}

interface RiskFactor {
    name: string
    points: number
    explanation: string
    severity: 'info' | 'warning' | 'critical'
    icon: string
    requires_validation: boolean
}

interface Recommendation {
    technique: string
    priority: string
    reason: string
    impact: string
}

interface ScoreBreakdown {
    base: number
    benefit_points: number
    risk_points: number
    formula: string
}

interface SyntheticSuitabilityProps {
    datasetId: string
}

export default function SyntheticSuitability({ datasetId }: SyntheticSuitabilityProps) {
    const [score, setScore] = useState<number>(50)
    const [verdict, setVerdict] = useState<string>('')
    const [riskLevel, setRiskLevel] = useState<string>('safe')
    const [dataType, setDataType] = useState<string>('general')
    const [breakdown, setBreakdown] = useState<ScoreBreakdown | null>(null)
    const [benefits, setBenefits] = useState<BenefitFactor[]>([])
    const [risks, setRisks] = useState<RiskFactor[]>([])
    const [recommendations, setRecommendations] = useState<Recommendation[]>([])
    const [explanation, setExplanation] = useState<string>('')
    const [cautionFlags, setCautionFlags] = useState<string[]>([])
    const [requiresValidation, setRequiresValidation] = useState<boolean>(false)
    const [loading, setLoading] = useState(true)
    const [expandedSection, setExpandedSection] = useState<string | null>('benefits')

    useEffect(() => {
        if (datasetId) {
            fetchData()
        }
    }, [datasetId])

    const fetchData = async () => {
        setLoading(true)
        try {
            const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
            const res = await fetch(`${apiUrl}/api/datasets/${datasetId}/synthetic`)
            const data = await res.json()

            if (data.status === 'success') {
                setScore(data.score || 50)
                setVerdict(data.verdict || '')
                setRiskLevel(data.risk_level || 'safe')
                setDataType(data.data_type || 'general')
                setBreakdown(data.score_breakdown || null)
                setBenefits(data.benefits || [])
                setRisks(data.risks || [])
                setRecommendations(data.recommendations || [])
                setExplanation(data.explanation || '')
                setCautionFlags(data.caution_flags || [])
                setRequiresValidation(data.requires_human_validation || false)
            }
        } catch (err) {
            console.error('Failed to load synthetic analysis:', err)
        } finally {
            setLoading(false)
        }
    }

    const getScoreColor = () => {
        if (riskLevel === 'high_risk') return 'text-red-500'
        if (riskLevel === 'caution') return 'text-yellow-500'
        if (score >= 70) return 'text-emerald-500'
        if (score >= 50) return 'text-blue-500'
        return 'text-gray-500'
    }

    const getVerdictBadge = () => {
        if (riskLevel === 'high_risk')
            return 'bg-red-100 dark:bg-red-900/30 text-red-600 dark:text-red-400 border-red-200 dark:border-red-800'
        if (riskLevel === 'caution')
            return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-600 dark:text-yellow-400 border-yellow-200 dark:border-yellow-800'
        if (score >= 70)
            return 'bg-emerald-100 dark:bg-emerald-900/30 text-emerald-600 dark:text-emerald-400 border-emerald-200 dark:border-emerald-800'
        return 'bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 border-blue-200 dark:border-blue-800'
    }

    const getSeverityIcon = (severity: string) => {
        switch (severity) {
            case 'critical':
                return <ShieldExclamationIcon className="w-5 h-5 text-red-500" />
            case 'warning':
                return <ExclamationTriangleIcon className="w-5 h-5 text-yellow-500" />
            default:
                return <InformationCircleIcon className="w-5 h-5 text-blue-500" />
        }
    }

    const toggleSection = (section: string) => {
        setExpandedSection(expandedSection === section ? null : section)
    }

    if (loading) {
        return (
            <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700">
                <h3 className="text-xl font-bold text-gray-900 dark:text-white mb-4">
                    Synthetic Data Analysis
                </h3>
                <div className="animate-pulse space-y-4">
                    <div className="h-24 bg-gray-200 dark:bg-gray-700 rounded-xl" />
                    <div className="h-32 bg-gray-200 dark:bg-gray-700 rounded-xl" />
                </div>
            </div>
        )
    }

    return (
        <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700">
            {/* Header with Verdict */}
            <div className="flex items-start justify-between mb-6">
                <div>
                    <h3 className="text-xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                        <ScaleIcon className="w-6 h-6 text-purple-500" />
                        Synthetic Data Analysis
                    </h3>
                    <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                        Benefit vs. Risk Assessment
                    </p>
                </div>
                {requiresValidation && (
                    <span className="px-3 py-1 rounded-full text-xs font-bold bg-amber-100 dark:bg-amber-900/30 text-amber-600 dark:text-amber-400 flex items-center gap-1">
                        <ExclamationTriangleIcon className="w-4 h-4" />
                        Validation Required
                    </span>
                )}
            </div>

            {/* Score and Verdict Display */}
            <div className={`p-4 rounded-xl mb-6 border ${getVerdictBadge()}`}>
                <div className="flex items-center gap-6">
                    {/* Score Circle */}
                    <div className="relative w-24 h-24 flex-shrink-0">
                        <svg className="w-full h-full transform -rotate-90">
                            <circle
                                cx="48"
                                cy="48"
                                r="42"
                                className="fill-none stroke-gray-200 dark:stroke-gray-700"
                                strokeWidth="8"
                            />
                            <circle
                                cx="48"
                                cy="48"
                                r="42"
                                className={`fill-none ${riskLevel === 'high_risk' ? 'stroke-red-500' :
                                        riskLevel === 'caution' ? 'stroke-yellow-500' :
                                            score >= 70 ? 'stroke-emerald-500' : 'stroke-blue-500'
                                    }`}
                                strokeWidth="8"
                                strokeLinecap="round"
                                strokeDasharray={`${(score / 100) * 264} 264`}
                            />
                        </svg>
                        <div className="absolute inset-0 flex items-center justify-center">
                            <span className={`text-3xl font-bold ${getScoreColor()}`}>
                                {Math.round(score)}
                            </span>
                        </div>
                    </div>

                    {/* Verdict and Formula */}
                    <div className="flex-1">
                        <p className="text-lg font-bold text-gray-900 dark:text-white mb-2">
                            {verdict}
                        </p>
                        {breakdown && (
                            <div className="text-sm text-gray-600 dark:text-gray-400 font-mono bg-white/50 dark:bg-gray-800/50 rounded px-2 py-1 inline-block">
                                {breakdown.formula}
                            </div>
                        )}
                    </div>
                </div>
            </div>

            {/* Caution Flags */}
            {cautionFlags.length > 0 && (
                <div className="mb-6 p-4 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded-xl">
                    <h4 className="font-semibold text-amber-700 dark:text-amber-400 mb-2 flex items-center gap-2">
                        <ExclamationTriangleIcon className="w-5 h-5" />
                        Caution Flags
                    </h4>
                    <ul className="space-y-1 text-sm text-amber-800 dark:text-amber-300">
                        {cautionFlags.map((flag, idx) => (
                            <li key={idx}>{flag}</li>
                        ))}
                    </ul>
                </div>
            )}

            {/* Benefits Section */}
            {benefits.length > 0 && (
                <div className="mb-4">
                    <button
                        onClick={() => toggleSection('benefits')}
                        className="w-full flex items-center justify-between p-3 bg-emerald-50 dark:bg-emerald-900/20 rounded-xl hover:bg-emerald-100 dark:hover:bg-emerald-900/30 transition-colors"
                    >
                        <div className="flex items-center gap-2">
                            <CheckCircleIcon className="w-5 h-5 text-emerald-500" />
                            <span className="font-semibold text-gray-900 dark:text-white">
                                Benefits ({benefits.length})
                            </span>
                            <span className="text-sm text-emerald-600 dark:text-emerald-400">
                                +{breakdown?.benefit_points || 0} pts
                            </span>
                        </div>
                        {expandedSection === 'benefits' ? (
                            <ChevronUpIcon className="w-5 h-5 text-gray-400" />
                        ) : (
                            <ChevronDownIcon className="w-5 h-5 text-gray-400" />
                        )}
                    </button>

                    {expandedSection === 'benefits' && (
                        <div className="mt-2 space-y-2">
                            {benefits.map((benefit, idx) => (
                                <div
                                    key={idx}
                                    className="p-3 bg-white dark:bg-gray-800 border border-emerald-100 dark:border-emerald-900/50 rounded-lg"
                                >
                                    <div className="flex items-center justify-between mb-1">
                                        <span className="font-medium text-gray-900 dark:text-white flex items-center gap-2">
                                            <span>{benefit.icon}</span>
                                            {benefit.name}
                                        </span>
                                        <span className="text-sm font-bold text-emerald-600 dark:text-emerald-400">
                                            +{benefit.points}
                                        </span>
                                    </div>
                                    <p className="text-sm text-gray-600 dark:text-gray-400">
                                        {benefit.explanation}
                                    </p>
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            )}

            {/* Risks Section */}
            {risks.length > 0 && (
                <div className="mb-4">
                    <button
                        onClick={() => toggleSection('risks')}
                        className="w-full flex items-center justify-between p-3 bg-red-50 dark:bg-red-900/20 rounded-xl hover:bg-red-100 dark:hover:bg-red-900/30 transition-colors"
                    >
                        <div className="flex items-center gap-2">
                            <ShieldExclamationIcon className="w-5 h-5 text-red-500" />
                            <span className="font-semibold text-gray-900 dark:text-white">
                                Risks ({risks.length})
                            </span>
                            <span className="text-sm text-red-600 dark:text-red-400">
                                -{breakdown?.risk_points || 0} pts
                            </span>
                        </div>
                        {expandedSection === 'risks' ? (
                            <ChevronUpIcon className="w-5 h-5 text-gray-400" />
                        ) : (
                            <ChevronDownIcon className="w-5 h-5 text-gray-400" />
                        )}
                    </button>

                    {expandedSection === 'risks' && (
                        <div className="mt-2 space-y-2">
                            {risks.map((risk, idx) => (
                                <div
                                    key={idx}
                                    className={`p-3 bg-white dark:bg-gray-800 border rounded-lg ${risk.severity === 'critical'
                                            ? 'border-red-200 dark:border-red-900/50'
                                            : risk.severity === 'warning'
                                                ? 'border-yellow-200 dark:border-yellow-900/50'
                                                : 'border-gray-200 dark:border-gray-700'
                                        }`}
                                >
                                    <div className="flex items-center justify-between mb-1">
                                        <span className="font-medium text-gray-900 dark:text-white flex items-center gap-2">
                                            {getSeverityIcon(risk.severity)}
                                            {risk.name}
                                        </span>
                                        <span className="text-sm font-bold text-red-600 dark:text-red-400">
                                            -{risk.points}
                                        </span>
                                    </div>
                                    <p className="text-sm text-gray-600 dark:text-gray-400">
                                        {risk.explanation}
                                    </p>
                                    {risk.requires_validation && (
                                        <span className="inline-flex items-center gap-1 mt-2 px-2 py-0.5 bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400 rounded-full text-xs">
                                            <ExclamationTriangleIcon className="w-3 h-3" />
                                            Requires Human Validation
                                        </span>
                                    )}
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            )}

            {/* Recommendations Section */}
            {recommendations.length > 0 && (
                <div className="mb-4">
                    <button
                        onClick={() => toggleSection('recommendations')}
                        className="w-full flex items-center justify-between p-3 bg-blue-50 dark:bg-blue-900/20 rounded-xl hover:bg-blue-100 dark:hover:bg-blue-900/30 transition-colors"
                    >
                        <div className="flex items-center gap-2">
                            <BeakerIcon className="w-5 h-5 text-blue-500" />
                            <span className="font-semibold text-gray-900 dark:text-white">
                                Recommended Techniques ({recommendations.length})
                            </span>
                        </div>
                        {expandedSection === 'recommendations' ? (
                            <ChevronUpIcon className="w-5 h-5 text-gray-400" />
                        ) : (
                            <ChevronDownIcon className="w-5 h-5 text-gray-400" />
                        )}
                    </button>

                    {expandedSection === 'recommendations' && (
                        <div className="mt-2 space-y-2">
                            {recommendations.map((rec, idx) => (
                                <div
                                    key={idx}
                                    className="p-3 bg-white dark:bg-gray-800 border border-blue-100 dark:border-blue-900/50 rounded-lg flex items-center justify-between"
                                >
                                    <div>
                                        <span className="font-medium text-gray-900 dark:text-white">
                                            {rec.technique}
                                        </span>
                                        <p className="text-sm text-gray-500 dark:text-gray-400">
                                            {rec.reason}
                                        </p>
                                    </div>
                                    <div className="text-right">
                                        <span className={`px-2 py-0.5 rounded text-xs font-bold ${rec.priority === 'critical' ? 'bg-red-500 text-white' :
                                                rec.priority === 'high' ? 'bg-emerald-500 text-white' :
                                                    'bg-blue-500 text-white'
                                            }`}>
                                            {rec.priority}
                                        </span>
                                        <p className="text-xs text-gray-500 mt-1">{rec.impact}</p>
                                    </div>
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            )}

            {/* Data Type Badge */}
            <div className="flex items-center gap-2 pt-4 border-t border-gray-200 dark:border-gray-700">
                <span className="text-sm text-gray-500">Data Type:</span>
                <span className="px-2 py-0.5 bg-primary-100 dark:bg-primary-900/30 text-primary-700 dark:text-primary-400 rounded-full text-sm font-medium capitalize">
                    {dataType}
                </span>
            </div>
        </div>
    )
}
