'use client'

import Badge from './Badge'
import { ArrowDownTrayIcon, HeartIcon, ServerStackIcon } from '@heroicons/react/24/outline'

interface Dataset {
    id: string
    name: string
    description: string
    domain?: string
    modality?: string
    trend_score?: number
    quality_score?: number
    size?: {
        samples?: number
        file_size_bytes?: number
        file_size_gb?: number
    }
    source?: {
        platform?: string
        source_metadata?: {
            downloads?: number
            likes?: number
            last_modified?: string
        }
    }
    intelligence?: {
        summary?: string
        tasks?: string[]
        modalities?: string[]
        domain?: string
        difficulty?: string
        quality_notes?: string[]
        tags?: string[]
    }
}

interface DatasetCardProps {
    dataset: Dataset
    onClick: (id: string) => void
}

// Helper function to format file size
const formatFileSize = (bytes?: number, gb?: number): string | null => {
    // Try bytes first (more accurate)
    if (bytes && bytes > 0) {
        if (bytes >= 1024 ** 3) {
            return `${(bytes / (1024 ** 3)).toFixed(1)} GB`
        } else if (bytes >= 1024 ** 2) {
            return `${(bytes / (1024 ** 2)).toFixed(0)} MB`
        } else if (bytes >= 1024) {
            return `${(bytes / 1024).toFixed(0)} KB`
        }
        return `${bytes} B`
    }
    // Fall back to GB field
    if (gb && gb > 0) {
        if (gb >= 1) {
            return `${gb.toFixed(1)} GB`
        }
        return `${(gb * 1024).toFixed(0)} MB`
    }
    return null
}

export default function DatasetCard({ dataset, onClick }: DatasetCardProps) {
    const fileSize = formatFileSize(dataset.size?.file_size_bytes, dataset.size?.file_size_gb)

    return (
        <div
            onClick={() => onClick(dataset.id)}
            className="bg-white dark:bg-gray-800 rounded-xl sm:rounded-2xl p-4 sm:p-6 shadow-sm dark:shadow-lg hover:shadow-md dark:hover:shadow-2xl hover:-translate-y-1 transition-all duration-300 border-2 border-gray-200 dark:border-gray-700 hover:border-purple-500 dark:hover:border-purple-400 hover:ring-1 hover:ring-purple-300/30 dark:hover:ring-purple-400/40 cursor-pointer relative"
        >
            {/* Header */}
            <div className="mb-3 sm:mb-4">
                <h4 className="text-base sm:text-lg font-bold text-gray-900 dark:text-white mb-1.5 sm:mb-2 line-clamp-2">
                    {dataset.name}
                </h4>
                <p className="text-gray-600 dark:text-gray-400 text-xs sm:text-sm line-clamp-3">
                    {dataset.description}
                </p>
            </div>

            {/* Badges */}
            <div className="flex gap-2 flex-wrap mb-4">
                {dataset.quality_score !== undefined && (
                    <Badge 
                        variant={
                            dataset.quality_score >= 0.8 ? 'success' :
                            dataset.quality_score >= 0.6 ? 'primary' :
                            dataset.quality_score >= 0.4 ? 'warning' : 'danger'
                        } 
                        size="sm"
                    >
                        Quality: {(dataset.quality_score * 100).toFixed(0)}%
                    </Badge>
                )}
                {dataset.domain && (
                    <Badge variant="domain" size="sm">
                        {dataset.domain}
                    </Badge>
                )}
                {dataset.modality && (
                    <Badge variant="modality" size="sm">
                        {dataset.modality}
                    </Badge>
                )}
            </div>

            {/* Statistics Row */}
            {(dataset.source?.source_metadata?.downloads !== undefined ||
                dataset.source?.source_metadata?.likes !== undefined) && (
                <div className="flex items-center gap-4 mb-4 text-sm">
                    {dataset.source?.source_metadata?.downloads !== undefined && (
                        <div className="flex items-center gap-1 text-gray-600 dark:text-gray-400">
                            <ArrowDownTrayIcon className="h-4 w-4" />
                            <span>
                                {dataset.source.source_metadata.downloads >= 1000000
                                    ? `${(dataset.source.source_metadata.downloads / 1000000).toFixed(1)}M`
                                    : dataset.source.source_metadata.downloads >= 1000
                                        ? `${(dataset.source.source_metadata.downloads / 1000).toFixed(1)}K`
                                        : dataset.source.source_metadata.downloads.toLocaleString()}
                            </span>
                        </div>
                    )}
                    {dataset.source?.source_metadata?.likes !== undefined && (
                        <div className="flex items-center gap-1 text-gray-600 dark:text-gray-400">
                            <HeartIcon className="h-4 w-4" />
                            <span>
                                {dataset.source.source_metadata.likes >= 1000
                                    ? `${(dataset.source.source_metadata.likes / 1000).toFixed(1)}K`
                                    : dataset.source.source_metadata.likes.toLocaleString()}
                            </span>
                        </div>
                    )}
                </div>
            )}

            {/* Trend Score */}
            {dataset.trend_score !== undefined && (
                <div>
                    <div className="flex items-center justify-between text-xs text-gray-600 dark:text-gray-400 mb-2">
                        <span>Trend Score</span>
                        <span className="font-bold text-gray-900 dark:text-white">
                            {Math.round(dataset.trend_score * 100)}%
                        </span>
                    </div>
                    <div className="relative h-2 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
                        <div
                            className="absolute inset-y-0 left-0 bg-gradient-to-r from-purple-500 to-pink-500 rounded-full transition-all duration-500"
                            style={{ width: `${dataset.trend_score * 100}%` }}
                        />
                    </div>
                </div>
            )}
        </div>
    )
}
