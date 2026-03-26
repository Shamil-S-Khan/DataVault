'use client'

import { useEffect, useState } from 'react'
import { CpuChipIcon, ClockIcon, ServerIcon } from '@heroicons/react/24/outline'

interface ModelRecommendation {
    name: string
    architecture: string
    category: string
    description: string
    why_suitable: string
    typical_use: string
    complexity: 'low' | 'medium' | 'high'
    training_time: 'fast' | 'medium' | 'slow'
    resources: 'low' | 'medium' | 'high'
    icon: string
}

interface ModelRecommendationsData {
    detected_task: string | null
    normalized_task: string | null
    modality: string
    recommendations: ModelRecommendation[]
    size_guidance: string
    count: number
}

interface ModelRecommendationsProps {
    datasetId: string
}

export default function ModelRecommendations({ datasetId }: ModelRecommendationsProps) {
    const [data, setData] = useState<ModelRecommendationsData | null>(null)
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)
    const [expandedModel, setExpandedModel] = useState<string | null>(null)

    useEffect(() => {
        if (datasetId) {
            fetchRecommendations()
        }
    }, [datasetId])

    const fetchRecommendations = async () => {
        setLoading(true)
        try {
            const apiUrl = (process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001').replace(/\/api\/?$/, '')
            const response = await fetch(`${apiUrl}/api/datasets/${datasetId}/models?limit=5`)
            const result = await response.json()

            if (result.status === 'success') {
                setData(result)
            } else {
                setError('Failed to load recommendations')
            }
        } catch (err) {
            setError('Failed to load recommendations')
        } finally {
            setLoading(false)
        }
    }

    const getComplexityStyles = (level: string) => {
        switch (level) {
            case 'low':
                return { bg: 'bg-emerald-100 dark:bg-emerald-900/30', text: 'text-emerald-700 dark:text-emerald-400' }
            case 'medium':
                return { bg: 'bg-yellow-100 dark:bg-yellow-900/30', text: 'text-yellow-700 dark:text-yellow-400' }
            case 'high':
                return { bg: 'bg-red-100 dark:bg-red-900/30', text: 'text-red-700 dark:text-red-400' }
            default:
                return { bg: 'bg-gray-100 dark:bg-gray-800', text: 'text-gray-700 dark:text-gray-400' }
        }
    }

    if (loading) {
        return (
            <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700">
                <h3 className="text-xl font-bold text-gray-900 dark:text-white mb-4">Recommended Models</h3>
                <div className="space-y-4">
                    {[1, 2, 3].map((i) => (
                        <div key={i} className="h-24 bg-gray-200 dark:bg-gray-700 rounded-xl animate-pulse" />
                    ))}
                </div>
            </div>
        )
    }

    if (error || !data || data.recommendations.length === 0) {
        return (
            <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700">
                <h3 className="text-xl font-bold text-gray-900 dark:text-white mb-4">Recommended Models</h3>
                <div className="text-center py-8">
                    <CpuChipIcon className="w-12 h-12 text-gray-400 mx-auto mb-3" />
                    <p className="text-gray-500 dark:text-gray-400">
                        {error || 'No model recommendations available'}
                    </p>
                </div>
            </div>
        )
    }

    return (
        <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700">
            {/* Header */}
            <div className="flex items-start justify-between mb-6">
                <div>
                    <h3 className="text-xl font-bold text-gray-900 dark:text-white">Recommended Models</h3>
                    <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                        Based on {data.detected_task || data.modality} task
                    </p>
                </div>
                <span className="px-3 py-1 bg-primary-100 dark:bg-primary-900/30 text-primary-700 dark:text-primary-400 rounded-full text-sm font-medium">
                    {data.count} models
                </span>
            </div>

            {/* Size Guidance */}
            <div className="mb-6 p-4 bg-blue-50 dark:bg-blue-900/20 rounded-xl border border-blue-200 dark:border-blue-800">
                <p className="text-sm text-blue-700 dark:text-blue-400">
                    💡 {data.size_guidance}
                </p>
            </div>

            {/* Model Cards */}
            <div className="space-y-4">
                {data.recommendations.map((model, index) => {
                    const isExpanded = expandedModel === model.name
                    const complexityStyles = getComplexityStyles(model.complexity)

                    return (
                        <div
                            key={model.name}
                            className={`bg-gray-50 dark:bg-gray-800/50 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden transition-all ${isExpanded ? 'ring-2 ring-primary-500' : ''
                                }`}
                        >
                            {/* Model Header */}
                            <button
                                onClick={() => setExpandedModel(isExpanded ? null : model.name)}
                                className="w-full p-4 text-left hover:bg-gray-100 dark:hover:bg-gray-700/50 transition-colors"
                            >
                                <div className="flex items-start gap-4">
                                    {/* Icon */}
                                    <div className="text-3xl">{model.icon}</div>

                                    {/* Info */}
                                    <div className="flex-1 min-w-0">
                                        <div className="flex items-center gap-2 mb-1">
                                            <h4 className="font-bold text-gray-900 dark:text-white">
                                                {model.name}
                                            </h4>
                                            <span className="text-xs text-gray-500 dark:text-gray-400 bg-gray-200 dark:bg-gray-700 px-2 py-0.5 rounded">
                                                {model.category}
                                            </span>
                                        </div>
                                        <p className="text-sm text-gray-600 dark:text-gray-400">
                                            {model.architecture}
                                        </p>
                                        <p className="text-sm text-primary-600 dark:text-primary-400 mt-1">
                                            {model.why_suitable}
                                        </p>
                                    </div>

                                    {/* Quick Stats */}
                                    <div className="flex gap-2">
                                        <span className={`px-2 py-1 rounded text-xs font-medium ${complexityStyles.bg} ${complexityStyles.text}`}>
                                            {model.complexity}
                                        </span>
                                    </div>
                                </div>
                            </button>

                            {/* Expanded Details */}
                            {isExpanded && (
                                <div className="px-4 pb-4 border-t border-gray-200 dark:border-gray-700">
                                    <div className="pt-4 space-y-4">
                                        {/* Description */}
                                        <p className="text-gray-600 dark:text-gray-400">
                                            {model.description}
                                        </p>

                                        {/* Typical Use */}
                                        <div>
                                            <h5 className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-1">
                                                Typical Use Case
                                            </h5>
                                            <p className="text-sm text-gray-600 dark:text-gray-400">
                                                {model.typical_use}
                                            </p>
                                        </div>

                                        {/* Resource Requirements */}
                                        <div className="grid grid-cols-3 gap-4">
                                            <div className="text-center p-3 bg-gray-100 dark:bg-gray-700/50 rounded-lg">
                                                <CpuChipIcon className="w-5 h-5 mx-auto mb-1 text-gray-500" />
                                                <p className="text-xs text-gray-500 dark:text-gray-400">Complexity</p>
                                                <p className={`text-sm font-semibold capitalize ${getComplexityStyles(model.complexity).text}`}>
                                                    {model.complexity}
                                                </p>
                                            </div>
                                            <div className="text-center p-3 bg-gray-100 dark:bg-gray-700/50 rounded-lg">
                                                <ClockIcon className="w-5 h-5 mx-auto mb-1 text-gray-500" />
                                                <p className="text-xs text-gray-500 dark:text-gray-400">Training</p>
                                                <p className={`text-sm font-semibold capitalize ${getComplexityStyles(model.training_time).text}`}>
                                                    {model.training_time}
                                                </p>
                                            </div>
                                            <div className="text-center p-3 bg-gray-100 dark:bg-gray-700/50 rounded-lg">
                                                <ServerIcon className="w-5 h-5 mx-auto mb-1 text-gray-500" />
                                                <p className="text-xs text-gray-500 dark:text-gray-400">Resources</p>
                                                <p className={`text-sm font-semibold capitalize ${getComplexityStyles(model.resources).text}`}>
                                                    {model.resources}
                                                </p>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            )}
                        </div>
                    )
                })}
            </div>
        </div>
    )
}
