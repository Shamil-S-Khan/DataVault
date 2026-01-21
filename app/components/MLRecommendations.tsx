'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import { SparklesIcon, ArrowRightIcon } from '@heroicons/react/24/outline'
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
}

export default function MLRecommendations({ 
    datasetId, 
    sameModality = false, 
    samePlatform = false,
    limit = 6 
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
            const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
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
            setRecommendations(data.recommendations || [])
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Unknown error')
        } finally {
            setLoading(false)
        }
    }

    if (loading) {
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

    if (error) {
        return (
            <div className="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-xl p-6">
                <p className="text-yellow-800 dark:text-yellow-200 text-sm">
                    ⚠️ Recommendations not available. Run: <code className="bg-yellow-100 dark:bg-yellow-900 px-2 py-1 rounded">python -m app.generate_embeddings</code>
                </p>
            </div>
        )
    }

    if (recommendations.length === 0) {
        return null
    }

    return (
        <div className="bg-gradient-to-br from-primary-50 to-purple-50 dark:from-gray-800 dark:to-gray-900 rounded-xl p-6 shadow-sm border border-primary-200 dark:border-gray-700">
            <div className="flex items-center gap-2 mb-4">
                <SparklesIcon className="w-6 h-6 text-primary-600 dark:text-primary-400" />
                <h3 className="text-lg font-bold text-gray-900 dark:text-white">
                    ML-Powered Recommendations
                </h3>
                <span className="ml-2 px-2 py-0.5 bg-primary-600 text-white text-xs font-semibold rounded-full">
                    NEW
                </span>
            </div>
            <p className="text-sm text-gray-600 dark:text-gray-400 mb-4">
                Semantically similar datasets based on AI embeddings
            </p>

            <div className="space-y-3">
                {recommendations.map((rec) => (
                    <Link
                        key={rec.id}
                        href={`/dataset/${rec.id}`}
                        className="block bg-white dark:bg-gray-800 rounded-lg p-4 border border-gray-200 dark:border-gray-700 hover:border-primary-400 dark:hover:border-primary-600 hover:shadow-lg transition-all duration-300 group"
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
                                <div className="flex items-center gap-2 flex-wrap">
                                    {rec.domain && (
                                        <Badge variant="domain" size="xs">
                                            {rec.domain}
                                        </Badge>
                                    )}
                                    {rec.modality && (
                                        <Badge variant="modality" size="xs">
                                            {rec.modality}
                                        </Badge>
                                    )}
                                </div>
                            </div>
                            <div className="flex flex-col items-end gap-1">
                                <div className="flex items-center gap-1">
                                    <span className="text-xs text-gray-500 dark:text-gray-400">Similarity</span>
                                    <div className="flex items-center gap-1 px-2 py-1 bg-primary-100 dark:bg-primary-900/30 rounded-full">
                                        <span className="text-sm font-bold text-primary-700 dark:text-primary-300">
                                            {(rec.similarity_score * 100).toFixed(0)}%
                                        </span>
                                    </div>
                                </div>
                                <ArrowRightIcon className="w-4 h-4 text-gray-400 group-hover:text-primary-600 dark:group-hover:text-primary-400 group-hover:translate-x-1 transition-all" />
                            </div>
                        </div>
                    </Link>
                ))}
            </div>
        </div>
    )
}
