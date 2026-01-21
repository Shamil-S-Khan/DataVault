'use client'

import { useState } from 'react'
import {
    ChevronDownIcon,
    ChevronUpIcon,
    PhotoIcon,
    MagnifyingGlassPlusIcon,
    TagIcon,
    DocumentTextIcon,
    IdentificationIcon
} from '@heroicons/react/24/outline'
import DataTypeBadge from './DataTypeBadge'

interface DatasetSampleCardProps {
    sample: any
    index: number
    onImageClick?: (imageUrl: string) => void
}

export default function DatasetSampleCard({ sample, index, onImageClick }: DatasetSampleCardProps) {
    const [isExpanded, setIsExpanded] = useState(true) // Show content by default
    const [showRawView, setShowRawView] = useState(false)

    // Extract and categorize fields
    const categorizeFields = () => {
        const media: any[] = []
        const identifiers: any[] = []
        const labels: any[] = []
        const descriptions: any[] = []
        const other: any[] = []

        Object.entries(sample.data || {}).forEach(([key, value]) => {
            const lowerKey = key.toLowerCase()

            // Media fields - check for image, audio, video
            if (lowerKey.includes('image') || lowerKey.includes('photo') || lowerKey.includes('picture') ||
                lowerKey.includes('audio') || lowerKey.includes('sound') || lowerKey.includes('music') ||
                lowerKey.includes('video') || lowerKey.includes('movie') || lowerKey.includes('clip')) {
                media.push({ key, value })
            }
            // Identifier fields
            else if (lowerKey.includes('id') || lowerKey === 'index' || lowerKey === 'row_idx') {
                identifiers.push({ key, value })
            }
            // Label/Classification fields
            else if (lowerKey.includes('label') || lowerKey.includes('category') || lowerKey.includes('class') ||
                lowerKey.includes('breed') || lowerKey.includes('type')) {
                labels.push({ key, value })
            }
            // Description fields
            else if (lowerKey.includes('caption') || lowerKey.includes('description') || lowerKey.includes('text') ||
                lowerKey.includes('enriched')) {
                descriptions.push({ key, value })
            }
            // Everything else
            else {
                other.push({ key, value })
            }
        })

        return { media, identifiers, labels, descriptions, other }
    }

    const { media, identifiers, labels, descriptions, other } = categorizeFields()

    // Prioritize media objects with src property over plain strings
    const primaryImage = media.find(m => typeof m.value === 'object' && m.value?.src)?.value || media[0]?.value

    const renderValue = (value: any) => {
        if (value === null || value === undefined) {
            return <span className="text-sm text-gray-400 italic">null</span>
        }

        // Handle arrays (could be array of images, audio, etc.)
        if (Array.isArray(value)) {
            return (
                <div className="space-y-2 mt-2">
                    {value.map((item, idx) => (
                        <div key={idx}>
                            {renderValue(item)}
                        </div>
                    ))}
                </div>
            )
        }

        // Handle media objects (image, audio, video with src and type)
        if (typeof value === 'object' && value !== null && value.src) {
            const src = value.src
            const type = value.type || ''

            // Image - check type OR file extension OR presence of width/height
            if (type.includes('image') ||
                src.match(/\.(jpg|jpeg|png|gif|webp|bmp|svg)$/i) ||
                (value.width && value.height)) {
                return (
                    <div className="relative group mt-2">
                        <img
                            src={src}
                            alt="Sample"
                            className="w-full h-48 object-cover rounded-lg cursor-pointer hover:opacity-90 transition-opacity"
                            onClick={() => onImageClick?.(src)}
                        />
                        <div className="absolute inset-0 bg-black/0 group-hover:bg-black/20 transition-colors rounded-lg flex items-center justify-center">
                            <MagnifyingGlassPlusIcon className="h-8 w-8 text-white opacity-0 group-hover:opacity-100 transition-opacity" />
                        </div>
                    </div>
                )
            }

            // Audio
            if (type.includes('audio') || src.match(/\.(mp3|wav|ogg|m4a)$/i)) {
                return (
                    <div className="mt-2">
                        <audio controls className="w-full" preload="metadata">
                            <source src={src} type={type || 'audio/mpeg'} />
                            Your browser does not support the audio element.
                        </audio>
                    </div>
                )
            }

            // Video
            if (type.includes('video') || src.match(/\.(mp4|webm|ogg)$/i)) {
                return (
                    <div className="mt-2">
                        <video controls className="w-full h-64 rounded-lg bg-black" preload="metadata">
                            <source src={src} type={type || 'video/mp4'} />
                            Your browser does not support the video element.
                        </video>
                    </div>
                )
            }
        }

        // Handle direct URL strings
        if (typeof value === 'string' && (value.startsWith('http://') || value.startsWith('https://'))) {
            // Image URL
            if (value.match(/\.(jpg|jpeg|png|gif|webp)$/i)) {
                return (
                    <div className="relative group mt-2">
                        <img
                            src={value}
                            alt="Sample"
                            className="w-full h-48 object-cover rounded-lg cursor-pointer hover:opacity-90 transition-opacity"
                            onClick={() => onImageClick?.(value)}
                        />
                        <div className="absolute inset-0 bg-black/0 group-hover:bg-black/20 transition-colors rounded-lg flex items-center justify-center">
                            <MagnifyingGlassPlusIcon className="h-8 w-8 text-white opacity-0 group-hover:opacity-100 transition-opacity" />
                        </div>
                    </div>
                )
            }

            // Audio URL
            if (value.match(/\.(mp3|wav|ogg|m4a)$/i)) {
                return (
                    <div className="mt-2">
                        <audio controls className="w-full">
                            <source src={value} />
                            Your browser does not support the audio element.
                        </audio>
                    </div>
                )
            }

            // Video URL
            if (value.match(/\.(mp4|webm|ogg)$/i)) {
                return (
                    <div className="mt-2">
                        <video controls className="w-full h-64 rounded-lg bg-black">
                            <source src={value} />
                            Your browser does not support the video element.
                        </video>
                    </div>
                )
            }

            // Other URLs
            return <a href={value} target="_blank" rel="noopener noreferrer" className="text-primary-600 dark:text-primary-400 hover:underline text-sm break-all">{value}</a>
        }

        // Handle other objects
        if (typeof value === 'object' && value !== null) {
            return <pre className="text-xs bg-gray-50 dark:bg-gray-800 p-3 rounded overflow-x-auto mt-2 text-gray-900 dark:text-gray-100 border border-gray-200 dark:border-gray-700">{JSON.stringify(value, null, 2)}</pre>
        }

        if (typeof value === 'boolean') {
            return <span className="text-sm font-medium text-gray-900 dark:text-white">{value ? 'true' : 'false'}</span>
        }

        if (typeof value === 'number') {
            return <span className="text-sm font-medium text-gray-900 dark:text-white">{value.toLocaleString()}</span>
        }

        return <span className="text-sm text-gray-900 dark:text-white">{String(value)}</span>
    }

    return (
        <div className="bg-white dark:bg-gray-800 rounded-2xl border border-gray-200 dark:border-gray-700 overflow-hidden hover:shadow-lg transition-shadow">
            {/* Header */}
            <div className="px-6 py-4 bg-gray-50 dark:bg-gray-900/50 border-b border-gray-200 dark:border-gray-700 flex items-center justify-between">
                <div className="flex items-center gap-3">
                    <span className="px-3 py-1 bg-primary-100 dark:bg-primary-900/30 text-primary-700 dark:text-primary-300 rounded-full text-sm font-bold">
                        Sample #{index + 1}
                    </span>
                    {identifiers.length > 0 && (
                        <span className="text-sm text-gray-500 dark:text-gray-400">
                            ID: {identifiers[0].value}
                        </span>
                    )}
                </div>
                <div className="flex items-center gap-2">
                    {isExpanded && (
                        <button
                            onClick={() => setShowRawView(!showRawView)}
                            className="px-3 py-1 text-xs font-medium text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white transition-colors"
                        >
                            {showRawView ? 'Formatted' : 'Raw'}
                        </button>
                    )}
                    <button
                        onClick={() => setIsExpanded(!isExpanded)}
                        className="p-2 hover:bg-gray-200 dark:hover:bg-gray-700 rounded-lg transition-colors"
                    >
                        {isExpanded ? (
                            <ChevronUpIcon className="h-5 w-5 text-gray-600 dark:text-gray-400" />
                        ) : (
                            <ChevronDownIcon className="h-5 w-5 text-gray-600 dark:text-gray-400" />
                        )}
                    </button>
                </div>
            </div>

            {/* Main Content - Two Column Layout */}
            <div className="p-6">
                {showRawView ? (
                    // Raw View
                    <div className="space-y-3">
                        {Object.entries(sample.data || {}).map(([key, value]) => (
                            <div key={key} className="flex flex-col gap-1">
                                <div className="flex items-center gap-2">
                                    <span className="text-xs font-mono text-gray-500 dark:text-gray-400">{key}</span>
                                    <DataTypeBadge type={typeof value} size="sm" />
                                </div>
                                <div className="pl-4">
                                    {renderValue(value)}
                                </div>
                            </div>
                        ))}
                    </div>
                ) : (
                    // Formatted View
                    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                        {/* Left Column - Image Preview */}
                        {primaryImage && (
                            <div className="lg:col-span-1">
                                <div className="sticky top-4">
                                    <div className="flex items-center gap-2 mb-3">
                                        <PhotoIcon className="h-4 w-4 text-gray-500" />
                                        <h4 className="text-sm font-semibold text-gray-700 dark:text-gray-300">Image</h4>
                                    </div>
                                    {renderValue(primaryImage)}
                                </div>
                            </div>
                        )}

                        {/* Right Column - Metadata */}
                        <div className={primaryImage ? 'lg:col-span-2' : 'lg:col-span-3'}>
                            <div className="space-y-6">
                                {/* Classification Labels */}
                                {labels.length > 0 && (
                                    <div>
                                        <div className="flex items-center gap-2 mb-3">
                                            <TagIcon className="h-4 w-4 text-gray-500" />
                                            <h4 className="text-sm font-semibold text-gray-700 dark:text-gray-300">Classification</h4>
                                        </div>
                                        <div className="flex flex-wrap gap-2">
                                            {labels.map(({ key, value }) => (
                                                <div key={key} className="inline-flex flex-col">
                                                    <span className="text-xs text-gray-500 dark:text-gray-400 mb-1">{key}</span>
                                                    <span className="px-4 py-2 bg-gradient-to-r from-purple-100 to-purple-200 dark:from-purple-900/30 dark:to-purple-800/30 text-purple-700 dark:text-purple-300 rounded-lg font-semibold text-sm border border-purple-200 dark:border-purple-700">
                                                        {String(value)}
                                                    </span>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                )}

                                {/* Descriptions */}
                                {descriptions.length > 0 && (
                                    <div>
                                        <div className="flex items-center gap-2 mb-3">
                                            <DocumentTextIcon className="h-4 w-4 text-gray-500" />
                                            <h4 className="text-sm font-semibold text-gray-700 dark:text-gray-300">Description</h4>
                                        </div>
                                        <div className="space-y-3">
                                            {descriptions.map(({ key, value }) => (
                                                <div key={key} className="bg-gray-50 dark:bg-gray-900/50 rounded-lg p-4 border border-gray-200 dark:border-gray-700">
                                                    <p className="text-xs text-gray-500 dark:text-gray-400 mb-1 font-medium">{key}</p>
                                                    <p className="text-sm text-gray-700 dark:text-gray-300 leading-relaxed">
                                                        {String(value)}
                                                    </p>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                )}

                                {/* Identifiers (if expanded) */}
                                {isExpanded && identifiers.length > 1 && (
                                    <div>
                                        <div className="flex items-center gap-2 mb-3">
                                            <IdentificationIcon className="h-4 w-4 text-gray-500" />
                                            <h4 className="text-sm font-semibold text-gray-700 dark:text-gray-300">Identifiers</h4>
                                        </div>
                                        <div className="grid grid-cols-2 gap-3">
                                            {identifiers.map(({ key, value }) => (
                                                <div key={key} className="flex flex-col">
                                                    <span className="text-xs text-gray-500 dark:text-gray-400">{key}</span>
                                                    <span className="text-sm font-mono text-gray-900 dark:text-white">{String(value)}</span>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                )}

                                {/* Other Fields (if expanded) */}
                                {isExpanded && other.length > 0 && (
                                    <div>
                                        <h4 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">Additional Fields</h4>
                                        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                                            {other.map(({ key, value }) => (
                                                <div key={key} className="flex flex-col gap-1">
                                                    <div className="flex items-center gap-2">
                                                        <span className="text-xs font-medium text-gray-600 dark:text-gray-400">{key}</span>
                                                        <DataTypeBadge type={typeof value} size="sm" />
                                                    </div>
                                                    <div className="text-sm">
                                                        {renderValue(value)}
                                                    </div>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>
                )}
            </div>
        </div>
    )
}
