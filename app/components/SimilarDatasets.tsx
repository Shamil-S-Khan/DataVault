'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import { ChevronLeftIcon, ChevronRightIcon, SparklesIcon } from '@heroicons/react/24/outline'

interface SimilarDataset {
    id: string
    name: string
    description: string
    domain?: string
    modality?: string
    platform?: string
    downloads?: number
    likes?: number
    license?: string
    similarity_score: number
    similarity_breakdown: {
        domain: number
        modality: number
        content: number
        tags: number
        tasks: number
    }
    match_reasons: string[]
}

interface SimilarDatasetsProps {
    datasetId: string
    compact?: boolean
}

export default function SimilarDatasets({ datasetId, compact = false }: SimilarDatasetsProps) {
    const [similar, setSimilar] = useState<SimilarDataset[]>([])
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)
    const [scrollPosition, setScrollPosition] = useState(0)

    useEffect(() => {
        if (datasetId) {
            fetchSimilarDatasets()
        }
    }, [datasetId])

    const fetchSimilarDatasets = async () => {
        setLoading(true)
        try {
            const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
            const response = await fetch(`${apiUrl}/api/datasets/${datasetId}/similar?limit=10`)
            const data = await response.json()

            if (data.status === 'success') {
                setSimilar(data.similar_datasets || [])
            } else {
                setError('Failed to load similar datasets')
            }
        } catch (err) {
            setError('Failed to load similar datasets')
        } finally {
            setLoading(false)
        }
    }

    const scroll = (direction: 'left' | 'right') => {
        const container = document.getElementById('similar-scroll-container')
        if (container) {
            const scrollAmount = 320
            const newPosition = direction === 'left'
                ? Math.max(0, scrollPosition - scrollAmount)
                : scrollPosition + scrollAmount
            container.scrollTo({ left: newPosition, behavior: 'smooth' })
            setScrollPosition(newPosition)
        }
    }

    const getSimilarityColor = (score: number) => {
        if (score >= 0.7) return 'text-emerald-500 bg-emerald-500/10'
        if (score >= 0.4) return 'text-yellow-500 bg-yellow-500/10'
        return 'text-gray-500 bg-gray-500/10'
    }

    if (loading) {
        return (
            <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700">
                <h3 className="text-xl font-bold text-gray-900 dark:text-white mb-4">Similar Datasets</h3>
                <div className="flex gap-4 overflow-hidden">
                    {[1, 2, 3].map((i) => (
                        <div key={i} className="flex-shrink-0 w-72 h-48 bg-gray-200 dark:bg-gray-700 rounded-xl animate-pulse" />
                    ))}
                </div>
            </div>
        )
    }

    if (error || similar.length === 0) {
        return (
            <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700">
                <h3 className="text-xl font-bold text-gray-900 dark:text-white mb-4">Similar Datasets</h3>
                <div className="text-center py-8">
                    <SparklesIcon className="w-12 h-12 text-gray-400 mx-auto mb-3" />
                    <p className="text-gray-500 dark:text-gray-400">
                        {error || 'No similar datasets found'}
                    </p>
                </div>
            </div>
        )
    }

    return (
        <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700">
            {/* Header */}
            <div className="flex items-center justify-between mb-4">
                <div>
                    <h3 className="text-xl font-bold text-gray-900 dark:text-white">Similar Datasets</h3>
                    <p className="text-sm text-gray-500 dark:text-gray-400">
                        Found {similar.length} related datasets
                    </p>
                </div>

                {/* Navigation Arrows */}
                <div className="flex gap-2">
                    <button
                        onClick={() => scroll('left')}
                        className="p-2 rounded-lg bg-gray-100 dark:bg-gray-800 hover:bg-gray-200 dark:hover:bg-gray-700 transition-colors"
                    >
                        <ChevronLeftIcon className="w-5 h-5 text-gray-600 dark:text-gray-400" />
                    </button>
                    <button
                        onClick={() => scroll('right')}
                        className="p-2 rounded-lg bg-gray-100 dark:bg-gray-800 hover:bg-gray-200 dark:hover:bg-gray-700 transition-colors"
                    >
                        <ChevronRightIcon className="w-5 h-5 text-gray-600 dark:text-gray-400" />
                    </button>
                </div>
            </div>

            {/* Carousel */}
            <div
                id="similar-scroll-container"
                className="flex gap-4 overflow-x-auto scrollbar-hide pb-2"
                style={{ scrollbarWidth: 'none', msOverflowStyle: 'none' }}
                onScroll={(e) => setScrollPosition((e.target as HTMLElement).scrollLeft)}
            >
                {similar.map((dataset) => (
                    <Link
                        key={dataset.id}
                        href={`/dataset/${dataset.id}`}
                        className="flex-shrink-0 w-72 group"
                    >
                        <div className="h-full bg-gray-50 dark:bg-gray-800/50 rounded-xl p-4 border border-gray-200 dark:border-gray-700 hover:border-primary-500 dark:hover:border-primary-500 transition-all hover:shadow-lg">
                            {/* Similarity Badge */}
                            <div className="flex items-center justify-between mb-3">
                                <span className={`px-2 py-1 rounded-full text-xs font-semibold ${getSimilarityColor(dataset.similarity_score)}`}>
                                    {Math.round(dataset.similarity_score * 100)}% match
                                </span>
                                {dataset.platform && (
                                    <span className="text-xs text-gray-500 dark:text-gray-400 capitalize">
                                        {dataset.platform}
                                    </span>
                                )}
                            </div>

                            {/* Dataset Name */}
                            <h4 className="font-semibold text-gray-900 dark:text-white mb-2 line-clamp-2 group-hover:text-primary-600 dark:group-hover:text-primary-400 transition-colors">
                                {dataset.name}
                            </h4>

                            {/* Description */}
                            <p className="text-sm text-gray-600 dark:text-gray-400 line-clamp-2 mb-3">
                                {dataset.description || 'No description available'}
                            </p>

                            {/* Match Reasons */}
                            <div className="flex flex-wrap gap-1 mb-3">
                                {dataset.match_reasons.slice(0, 3).map((reason, idx) => (
                                    <span
                                        key={idx}
                                        className="px-2 py-0.5 bg-primary-100 dark:bg-primary-900/30 text-primary-700 dark:text-primary-400 rounded-full text-xs"
                                    >
                                        {reason}
                                    </span>
                                ))}
                            </div>

                            {/* Stats */}
                            <div className="flex items-center gap-4 text-xs text-gray-500 dark:text-gray-400">
                                {dataset.domain && (
                                    <span className="capitalize">{dataset.domain}</span>
                                )}
                                {dataset.modality && (
                                    <span className="capitalize">{dataset.modality}</span>
                                )}
                                {dataset.downloads != null && (
                                    <span>{dataset.downloads.toLocaleString()} downloads</span>
                                )}
                            </div>

                        </div>
                    </Link>
                ))}
            </div>

            {/* Comparison View Toggle (Future Enhancement) */}
            <div className="mt-4 pt-4 border-t border-gray-200 dark:border-gray-700">
                <p className="text-xs text-gray-500 dark:text-gray-400 text-center">
                    Click any dataset to view details • Similarity based on domain, modality, content, and tags
                </p>
            </div>
        </div>
    )
}
