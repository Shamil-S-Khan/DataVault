'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import { SparklesIcon, ArrowRightIcon, ChevronDownIcon, ChevronUpIcon } from '@heroicons/react/24/outline'
import Badge from './Badge'

interface Recommendation {
    id: string
    name: string
    canonical_name: string
    description: string
    domain?: string
    modality?: string
    similarity_score: number
    trend_score?: number
    source?: {
        platform?: string
    }
    size?: {
        samples?: number
        file_size_gb?: number
    }
}

interface MLRecommendationsProps {
    datasetId: string
    sameModality?: boolean
    samePlatform?: boolean
    limit?: number
    variant?: 'default' | 'bottom-slider'
}

export default function MLRecommendations({
    datasetId,
    sameModality = false,
    samePlatform = false,
    limit = 6,
    variant = 'default'
}: MLRecommendationsProps) {
    const [recommendations, setRecommendations] = useState<Recommendation[]>([])
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)

    useEffect(() => {
        if (datasetId) {
            fetchRecommendations()
        }
    }, [datasetId, sameModality, samePlatform])

    const fetchRecommendations = async () => {
        setLoading(true)
        setError(null)
        try {
            const apiUrl = (process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001').replace(/\/api\/?$/, '')
            const params = new URLSearchParams({
                limit: limit.toString(),
                same_modality: sameModality.toString(),
                same_platform: samePlatform.toString()
            })
            const response = await fetch(
                `${apiUrl}/api/datasets/${datasetId}/similar?${params}`
            )
            if (!response.ok) throw new Error('Failed to fetch recommendations')
            const data = await response.json()
            setRecommendations(data.similar_datasets || [])
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Unknown error')
        } finally {
            setLoading(false)
        }
    }

    if (loading && variant === 'default') {
        return (
            <div className="bg-white dark:bg-gray-800 rounded-xl p-6 shadow-sm border border-gray-200 dark:border-gray-700">
                <div className="animate-pulse space-y-4">
                    <div className="h-6 bg-gray-200 dark:bg-gray-700 rounded w-1/3" />
                    <div className="space-y-3">
                        {[...Array(3)].map((_, i) => (
                            <div key={i} className="h-24 bg-gray-200 dark:bg-gray-700 rounded" />
                        ))}
                    </div>
                </div>
            </div>
        )
    }

    if (error && variant === 'default') {
        return (
            <div className="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-xl p-6">
                <p className="text-yellow-800 dark:text-yellow-200 text-sm">
                    ⚠️ Recommendations not available. Run: <code className="bg-yellow-100 dark:bg-yellow-900 px-2 py-1 rounded">python -m app.generate_embeddings</code>
                </p>
            </div>
        )
    }

    if (recommendations.length === 0 || (loading && variant === 'bottom-slider')) {
        return null
    }

    if (variant === 'bottom-slider') {
        return (
            <div className="w-full bg-white/50 dark:bg-gray-900/50 backdrop-blur-xl border-t border-gray-200 dark:border-gray-700 py-12 mt-12">
                <div className="max-w-7xl mx-auto">
                    <div className="flex items-center justify-between mb-4 px-2">
                        <div className="flex items-center gap-3">
                            <div className="p-2 bg-primary-100 dark:bg-primary-900/30 rounded-xl">
                                <SparklesIcon className="w-5 h-5 text-primary-600 animate-pulse" />
                            </div>
                            <div>
                                <h3 className="text-lg font-bold bg-gradient-to-r from-primary-600 to-purple-600 bg-clip-text text-transparent leading-none">
                                    Similar Datasets
                                </h3>
                                <p className="text-[10px] text-gray-500 dark:text-gray-400 mt-1 uppercase tracking-widest font-bold">
                                    AI-Powered Recommendations
                                </p>
                            </div>
                        </div>
                        <div className="flex items-center gap-2">
                            <span className="text-[10px] font-bold text-gray-400 uppercase tracking-widest bg-gray-100 dark:bg-gray-800 px-2 py-1 rounded-lg">
                                {recommendations.length} Suggestions
                            </span>
                        </div>
                    </div>
                    <div className="flex gap-4 overflow-x-auto pb-4 scrollbar-hide snap-x">
                        {recommendations.map((rec) => (
                            <Link
                                key={rec.id}
                                href={`/dataset/${rec.id}`}
                                className="flex-none w-80 group snap-start"
                            >
                                <div className="bg-white dark:bg-gray-800 rounded-2xl p-5 border border-gray-200 dark:border-gray-700 hover:border-primary-500 dark:hover:border-primary-500 hover:shadow-2xl hover:scale-[1.02] transition-all duration-300">
                                    <div className="flex items-start justify-between mb-3">
                                        <div className="flex-1 min-w-0">
                                            <h4 className="font-bold text-gray-900 dark:text-white group-hover:text-primary-600 transition-colors truncate">
                                                {rec.name}
                                            </h4>
                                            <div className="flex items-center gap-2 mt-1">
                                                {rec.source?.platform && (
                                                    <Badge variant="platform" size="xs">
                                                        {rec.source.platform}
                                                    </Badge>
                                                )}
                                                <span className="text-[10px] text-gray-400 uppercase font-bold tracking-tighter">
                                                    {(rec.similarity_score * 100).toFixed(0)}% Match
                                                </span>
                                            </div>
                                        </div>
                                        <div className="p-2 rounded-xl bg-primary-50 dark:bg-primary-900/20 group-hover:bg-primary-100 dark:group-hover:bg-primary-900/40 transition-colors">
                                            <ArrowRightIcon className="w-4 h-4 text-primary-600" />
                                        </div>
                                    </div>
                                    <p className="text-sm text-gray-600 dark:text-gray-400 line-clamp-2 min-h-[40px]">
                                        {rec.description}
                                    </p>
                                    <div className="flex items-center gap-2 mt-4">
                                        {rec.modality && <Badge variant="modality" size="xs">{rec.modality}</Badge>}
                                        {rec.domain && <Badge variant="domain" size="xs">{rec.domain}</Badge>}
                                    </div>
                                </div>
                            </Link>
                        ))}
                    </div>
                </div>
            </div>
        )
    }

    return (
        <div className="bg-gradient-to-br from-primary-50 to-purple-50 dark:from-gray-800 dark:to-gray-900 rounded-2xl p-6 shadow-xl border border-primary-100 dark:border-gray-700">
            <div className="flex items-center gap-2 mb-4">
                <SparklesIcon className="w-6 h-6 text-primary-600 dark:text-primary-400" />
                <h3 className="text-lg font-bold text-gray-900 dark:text-white">
                    ML-Powered Recommendations
                </h3>
            </div>
            <p className="text-sm text-gray-600 dark:text-gray-400 mb-4">
                Semantically similar datasets based on AI embeddings
            </p>

            <div className="grid grid-cols-1 gap-3">
                {recommendations.map((rec) => (
                    <Link
                        key={rec.id}
                        href={`/dataset/${rec.id}`}
                        className="block bg-white dark:bg-gray-800 rounded-xl p-4 border border-gray-200 dark:border-gray-700 hover:border-primary-400 dark:hover:border-primary-600 hover:shadow-lg transition-all duration-300 group"
                    >
                        <div className="flex items-start justify-between gap-3">
                            <div className="flex-1 min-w-0">
                                <div className="flex items-center gap-2 mb-1">
                                    <h4 className="font-semibold text-gray-900 dark:text-white text-sm truncate group-hover:text-primary-600 dark:group-hover:text-primary-400 transition-colors">
                                        {rec.name}
                                    </h4>
                                    {rec.source?.platform && (
                                        <Badge variant="platform" size="xs">
                                            {rec.source.platform}
                                        </Badge>
                                    )}
                                </div>
                                <p className="text-xs text-gray-600 dark:text-gray-400 line-clamp-2 mb-2">
                                    {rec.description}
                                </p>
                                <div className="flex items-center gap-2">
                                    {rec.domain && <Badge variant="domain" size="xs">{rec.domain}</Badge>}
                                    {rec.modality && <Badge variant="modality" size="xs">{rec.modality}</Badge>}
                                </div>
                            </div>
                            <div className="flex flex-col items-end gap-1">
                                <div className="flex items-center gap-1 px-2 py-1 bg-primary-50 dark:bg-primary-900/20 rounded-full">
                                    <span className="text-xs font-bold text-primary-700 dark:text-primary-300">
                                        {(rec.similarity_score * 100).toFixed(0)}%
                                    </span>
                                </div>
                                <ArrowRightIcon className="w-4 h-4 text-gray-400 group-hover:text-primary-600 transition-all" />
                            </div>
                        </div>
                    </Link>
                ))}
            </div>
        </div>
    )
}
