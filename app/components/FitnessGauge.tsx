'use client'

import { useEffect, useState } from 'react'

interface FitnessBreakdown {
    metadata_completeness: number
    size_appropriateness: number
    documentation_quality: number
    license_clarity: number
    freshness: number
    community_signals: number
}

interface TaskFitness {
    score: number
    max_score: number
    match_rate: number
    reasoning: string[]
}

interface FitnessData {
    overall_score: number
    grade: string
    breakdown: FitnessBreakdown
    task_fitness?: Record<string, TaskFitness>
    explanation: string
    calculated_at: string
}

interface FitnessGaugeProps {
    datasetId: string
    initialData?: FitnessData
    compact?: boolean
}

export default function FitnessGauge({ datasetId, initialData, compact = false }: FitnessGaugeProps) {
    const [fitness, setFitness] = useState<FitnessData | null>(initialData || null)
    const [loading, setLoading] = useState(!initialData)
    const [error, setError] = useState<string | null>(null)
    const [showBreakdown, setShowBreakdown] = useState(false)
    const [showTasks, setShowTasks] = useState(true)

    useEffect(() => {
        if (!initialData && datasetId) {
            fetchFitnessScore()
        }
    }, [datasetId, initialData])

    const fetchFitnessScore = async () => {
        setLoading(true)
        try {
            const apiUrl = (process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001').replace(/\/api\/?$/, '')
            const response = await fetch(`${apiUrl}/api/datasets/${datasetId}/fitness`)
            const data = await response.json()

            if (data.status === 'success') {
                setFitness(data.fitness)
            } else {
                setError('Failed to load fitness score')
            }
        } catch (err) {
            setError('Failed to load fitness score')
        } finally {
            setLoading(false)
        }
    }

    const getScoreColor = (score: number) => {
        if (score >= 8) return { bg: 'bg-emerald-500', text: 'text-emerald-500', ring: 'ring-emerald-500/30' }
        if (score >= 6) return { bg: 'bg-yellow-500', text: 'text-yellow-500', ring: 'ring-yellow-500/30' }
        if (score >= 4) return { bg: 'bg-orange-500', text: 'text-orange-500', ring: 'ring-orange-500/30' }
        return { bg: 'bg-red-500', text: 'text-red-500', ring: 'ring-red-500/30' }
    }

    const getGradeColor = (grade: string) => {
        if (grade.startsWith('A')) return 'text-emerald-500 bg-emerald-500/10'
        if (grade.startsWith('B')) return 'text-yellow-500 bg-yellow-500/10'
        if (grade.startsWith('C')) return 'text-orange-500 bg-orange-500/10'
        return 'text-red-500 bg-red-500/10'
    }

    const getDimensionLabel = (key: string) => {
        const labels: Record<string, string> = {
            metadata_completeness: 'Metadata',
            size_appropriateness: 'Size',
            documentation_quality: 'Documentation',
            license_clarity: 'License',
            freshness: 'Freshness',
            community_signals: 'Community'
        }
        return labels[key] || key
    }

    if (loading) {
        return (
            <div className={`${compact ? 'p-3' : 'p-6'} glass rounded-xl animate-pulse`}>
                <div className="flex items-center gap-4">
                    <div className={`${compact ? 'w-12 h-12' : 'w-24 h-24'} rounded-full bg-gray-300 dark:bg-gray-700`}></div>
                    <div className="flex-1 space-y-2">
                        <div className="h-4 bg-gray-300 dark:bg-gray-700 rounded w-24"></div>
                        <div className="h-3 bg-gray-300 dark:bg-gray-700 rounded w-32"></div>
                    </div>
                </div>
            </div>
        )
    }

    if (error || !fitness) {
        return (
            <div className={`${compact ? 'p-3' : 'p-6'} glass rounded-xl`}>
                <p className="text-gray-500 dark:text-gray-400 text-sm">Unable to calculate fitness score</p>
            </div>
        )
    }

    const colors = getScoreColor(fitness.overall_score)
    const circumference = 2 * Math.PI * 45
    const progress = (fitness.overall_score / 10) * circumference

    if (compact) {
        return (
            <div className="flex items-center gap-3">
                <div className="relative w-12 h-12">
                    <svg className="w-12 h-12 -rotate-90" viewBox="0 0 100 100">
                        <circle
                            cx="50"
                            cy="50"
                            r="45"
                            fill="none"
                            stroke="currentColor"
                            strokeWidth="8"
                            className="text-gray-200 dark:text-gray-700"
                        />
                        <circle
                            cx="50"
                            cy="50"
                            r="45"
                            fill="none"
                            stroke="currentColor"
                            strokeWidth="8"
                            strokeDasharray={circumference}
                            strokeDashoffset={circumference - progress}
                            strokeLinecap="round"
                            className={colors.text}
                        />
                    </svg>
                    <div className="absolute inset-0 flex items-center justify-center">
                        <span className={`text-sm font-bold ${colors.text}`}>{fitness.overall_score}</span>
                    </div>
                </div>
                <div>
                    <span className={`text-xs font-semibold px-2 py-0.5 rounded-full ${getGradeColor(fitness.grade)}`}>
                        {fitness.grade}
                    </span>
                    <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">Fitness</p>
                </div>
            </div>
        )
    }

    return (
        <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700 shadow-xl overflow-hidden">
            <div className="flex items-start gap-6 mb-8">
                {/* Circular Gauge */}
                <div className="relative shrink-0">
                    <div className={`w-28 h-28 rounded-full ${colors.ring} ring-8`}>
                        <svg className="w-28 h-28 -rotate-90" viewBox="0 0 100 100">
                            <circle
                                cx="50"
                                cy="50"
                                r="45"
                                fill="none"
                                stroke="currentColor"
                                strokeWidth="10"
                                className="text-gray-200 dark:text-gray-700"
                            />
                            <circle
                                cx="50"
                                cy="50"
                                r="45"
                                fill="none"
                                stroke="currentColor"
                                strokeWidth="10"
                                strokeDasharray={circumference}
                                strokeDashoffset={circumference - progress}
                                strokeLinecap="round"
                                className={`${colors.text} transition-all duration-1000`}
                            />
                        </svg>
                        <div className="absolute inset-0 flex flex-col items-center justify-center">
                            <span className={`text-3xl font-bold ${colors.text}`}>{fitness.overall_score}</span>
                            <span className="text-xs text-gray-500 dark:text-gray-400">fitness</span>
                        </div>
                    </div>
                </div>

                {/* Score Info */}
                <div className="flex-1 min-w-0">
                    <div className="flex flex-wrap items-center gap-3 mb-2">
                        <h3 className="text-xl font-bold text-gray-900 dark:text-white">Fitness Score</h3>
                        <span className={`px-3 py-1 rounded-full text-sm font-bold ${getGradeColor(fitness.grade)} whitespace-nowrap`}>
                            Grade: {fitness.grade}
                        </span>
                    </div>

                    <p className="text-gray-600 dark:text-gray-400 text-sm mb-4 leading-relaxed line-clamp-2">
                        {fitness.explanation}
                    </p>

                    <div className="flex items-center gap-4">
                        <button
                            onClick={() => setShowTasks(!showTasks)}
                            className="text-primary-600 dark:text-primary-400 text-xs font-semibold uppercase tracking-wider hover:underline"
                        >
                            {showTasks ? 'Hide Tasks' : 'Show Tasks'}
                        </button>
                        <button
                            onClick={() => setShowBreakdown(!showBreakdown)}
                            className="text-gray-500 dark:text-gray-400 text-xs font-semibold uppercase tracking-wider hover:underline"
                        >
                            {showBreakdown ? 'Hide Details' : 'Show Details'}
                        </button>
                    </div>
                </div>
            </div>

            {/* Task Fitness Breakdown */}
            {showTasks && fitness.task_fitness && (
                <div className="space-y-4 mb-6">
                    <h4 className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-widest">Target Task Alignment</h4>
                    <div className="grid grid-cols-1 gap-3">
                        {Object.entries(fitness.task_fitness).map(([task, data]) => (
                            <div key={task} className="bg-white/50 dark:bg-gray-800/50 rounded-xl p-4 border border-gray-100 dark:border-gray-700/50">
                                <div className="flex items-center justify-between mb-2">
                                    <span className="text-sm font-bold text-gray-900 dark:text-white">{task}</span>
                                    <span className={`text-sm font-black ${getScoreColor(data.score).text}`}>
                                        {data.match_rate}%
                                    </span>
                                </div>
                                <div className="w-full h-1.5 bg-gray-100 dark:bg-gray-900 rounded-full overflow-hidden mb-3">
                                    <div 
                                        className={`h-full ${getScoreColor(data.score).bg} transition-all duration-700`}
                                        style={{ width: `${data.match_rate}%` }}
                                    />
                                </div>
                                <div className="flex flex-wrap gap-x-3 gap-y-1">
                                    {data.reasoning.slice(0, 2).map((r, i) => (
                                        <div key={i} className="flex items-center gap-1.5 text-[10px] text-gray-500 dark:text-gray-400">
                                            <span className={r.includes('match') || r.includes('explicitly') ? 'text-emerald-500' : 'text-amber-500'}>
                                                {r.includes('match') || r.includes('explicitly') ? '●' : '○'}
                                            </span>
                                            {r}
                                        </div>
                                    ))}
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {/* General Breakdown Details */}
            {showBreakdown && (
                <div className="pt-6 border-t border-gray-200 dark:border-gray-700">
                    <h4 className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-widest mb-4">Metadata Quality Breakdown</h4>
                    <div className="grid grid-cols-2 gap-4">
                        {Object.entries(fitness.breakdown).map(([key, value]) => {
                            const dimColors = getScoreColor(value)
                            return (
                                <div key={key}>
                                    <div className="flex items-center justify-between mb-1">
                                        <span className="text-[10px] font-medium text-gray-500 dark:text-gray-400">
                                            {getDimensionLabel(key)}
                                        </span>
                                        <span className={`text-[10px] font-bold ${dimColors.text}`}>
                                            {value}/10
                                        </span>
                                    </div>
                                    <div className="w-full h-1 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
                                        <div
                                            className={`h-full ${dimColors.bg} transition-all duration-500`}
                                            style={{ width: `${(value / 10) * 100}%` }}
                                        />
                                    </div>
                                </div>
                            )
                        })}
                    </div>
                </div>
            )}
        </div>
    )
}

