'use client'

import { useState, useEffect } from 'react'
import { useParams, useRouter } from 'next/navigation'
import {
    ArrowLeftIcon, ArrowDownTrayIcon, GlobeAltIcon, TagIcon, CalendarIcon,
    EyeIcon, ChevronLeftIcon, ChevronRightIcon, HeartIcon, CloudArrowDownIcon,
    UserGroupIcon, ChartBarIcon, SparklesIcon, XMarkIcon, LinkIcon, ShieldCheckIcon
} from '@heroicons/react/24/outline'
import Badge from '../../components/Badge'
import DarkModeToggle from '../../components/DarkModeToggle'
import DatasetSummary from '../../components/DatasetSummary'
import TrustSignals from '../../components/TrustSignals'
import DataTypeBadge from '../../components/DataTypeBadge'
import DatasetSampleCard from '../../components/DatasetSampleCard'
import FitnessGauge from '../../components/FitnessGauge'
import LicenseBadge from '../../components/LicenseBadge'
import SimilarDatasets from '../../components/SimilarDatasets'
import ModelRecommendations from '../../components/ModelRecommendations'
import BiasChart from '../../components/BiasChart'
import VersionTimeline from '../../components/VersionTimeline'
import SyntheticSuitability from '../../components/SyntheticSuitability'
import DatasetCardGen from '../../components/DatasetCardGen'
import QualityScore from '../../components/QualityScore'
import GQIScore from '../../components/GQIScore'
import MLRecommendations from '../../components/MLRecommendations'

interface Dataset {
    id: string
    name: string
    description: string
    domain?: string
    modality?: string
    trend_score?: number
    quality_score?: number
    created_at?: string
    source?: {
        platform?: string
        platform_id?: string
        url?: string
        source_metadata?: any
    }
    size?: {
        samples?: number
        file_size_gb?: number
    }
    license?: string
    metadata?: any
    intelligence?: {
        summary?: string
        tasks?: string[]
        use_cases?: string[]
        modalities?: string[]
        domain?: string
        subdomains?: string[]
        fields?: Array<{
            name: string
            semantic_meaning: string
            data_type: string
            role: string
        }>
        labels?: {
            type?: string
            categories?: string[]
            annotation_method?: string
        }
        difficulty?: string
        quality_notes?: string[]
        ethical_flags?: string[]
        tags?: string[]
        analyzed_at?: string
        version?: string
    }
    intelligence_updated_at?: string
}

interface DatasetSample {
    row_idx: number
    row: any
    truncated_cells: string[]
}

interface SamplesResponse {
    samples: DatasetSample[]
    features: any[]
    pagination: {
        page: number
        limit: number
        total: number
        pages: number
    }
}

export default function DatasetDetailPage() {
    const params = useParams()
    const router = useRouter()
    const [dataset, setDataset] = useState<Dataset | null>(null)
    const [samples, setSamples] = useState<SamplesResponse | null>(null)
    const [loading, setLoading] = useState(true)
    const [samplesLoading, setSamplesLoading] = useState(false)
    const [error, setError] = useState<string | null>(null)
    const [samplesError, setSamplesError] = useState<string | null>(null)
    const [showSamples, setShowSamples] = useState(false)
    const [currentPage, setCurrentPage] = useState(1)
    const [imageModalUrl, setImageModalUrl] = useState<string | null>(null)
    const [activeTab, setActiveTab] = useState<'overview' | 'samples' | 'ai-analysis' | 'intelligence'>('overview')
    const [isAnalyzing, setIsAnalyzing] = useState(false)
    const [analysisError, setAnalysisError] = useState<string | null>(null)
    const [pollingInterval, setPollingInterval] = useState<NodeJS.Timeout | null>(null)
    const [pollingStartTime, setPollingStartTime] = useState<number | null>(null)
    const [pendingAutoSwitch, setPendingAutoSwitch] = useState(false) // Track if we should auto-switch after analysis


    useEffect(() => {
        if (params.id) {
            fetchDatasetDetails(params.id as string)
        }
    }, [params.id])

    // Cleanup polling on unmount
    useEffect(() => {
        return () => {
            if (pollingInterval) {
                clearInterval(pollingInterval)
            }
        }
    }, [pollingInterval])

    useEffect(() => {
        if (showSamples && dataset && !samples) {
            fetchSamples(1)
        }
    }, [showSamples, dataset])

    const fetchDatasetDetails = async (id: string) => {
        setLoading(true)
        setError(null)
        try {
            const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
            const response = await fetch(`${apiUrl}/api/datasets/${id}`)

            if (!response.ok) {
                throw new Error('Dataset not found')
            }

            const data = await response.json()
            setDataset(data)
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Failed to fetch dataset')
        } finally {
            setLoading(false)
        }
    }

    const fetchSamples = async (page: number) => {
        if (!dataset) return

        setSamplesLoading(true)
        setSamplesError(null)
        try {
            const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
            const response = await fetch(`${apiUrl}/api/datasets/${dataset.id}/samples?page=${page}&limit=10`)

            if (!response.ok) {
                const errorData = await response.json()
                throw new Error(errorData.detail || 'Failed to fetch samples')
            }

            const data = await response.json()
            setSamples(data)
            setCurrentPage(page)
        } catch (err) {
            setSamplesError(err instanceof Error ? err.message : 'Failed to fetch samples')
        } finally {
            setSamplesLoading(false)
        }
    }

    const pollForIntelligence = async (datasetId: string) => {
        try {
            // Check for timeout (30 seconds)
            if (pollingStartTime && Date.now() - pollingStartTime > 30000) {
                setIsAnalyzing(false)
                setAnalysisError('Analysis is taking longer than expected. Please try again or check back later.')
                if (pollingInterval) {
                    clearInterval(pollingInterval)
                    setPollingInterval(null)
                }
                setPollingStartTime(null)
                return false
            }

            const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
            const response = await fetch(`${apiUrl}/api/datasets/${datasetId}/intelligence`)

            if (response.ok) {
                const responseData = await response.json()

                // API returns: { status: "success", intelligence: {...} }
                if (responseData.status === 'success' && responseData.intelligence && responseData.intelligence.summary) {
                    // Intelligence is ready! Update the dataset
                    setDataset(prev => prev ? { ...prev, intelligence: responseData.intelligence } : null)
                    setIsAnalyzing(false)
                    setAnalysisError(null)
                    setPollingStartTime(null)

                    // Clear polling interval
                    if (pollingInterval) {
                        clearInterval(pollingInterval)
                        setPollingInterval(null)
                    }

                    // Only auto-switch if user initiated the analysis (not on page reload)
                    setPendingAutoSwitch(prev => {
                        if (prev) {
                            setActiveTab('ai-analysis')
                        }
                        return false
                    })

                    return true
                }
            }
            return false
        } catch (error) {
            console.error('Error polling for intelligence:', error)
            return false
        }
    }

    const startAnalysis = async () => {
        if (!dataset) return

        try {
            setIsAnalyzing(true)
            setAnalysisError(null)
            setPollingStartTime(Date.now())
            setPendingAutoSwitch(true) // User initiated, so we'll auto-switch when done

            const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
            const response = await fetch(`${apiUrl}/api/datasets/${dataset.id}/analyze`, {
                method: 'POST'
            })

            if (!response.ok) {
                throw new Error('Failed to start analysis')
            }

            const result = await response.json()

            if (result.status === 'queued') {
                // Start polling every 3 seconds
                const interval = setInterval(() => {
                    pollForIntelligence(dataset.id)
                }, 3000)
                setPollingInterval(interval)

                // Also poll immediately
                setTimeout(() => pollForIntelligence(dataset.id), 1000)
            } else if (result.status === 'already_analyzed') {
                // Already analyzed, just switch to the tab
                setActiveTab('ai-analysis')
                setIsAnalyzing(false)
                setPollingStartTime(null)
            }
        } catch (error) {
            console.error('Failed to start analysis:', error)
            setIsAnalyzing(false)
            setAnalysisError('Failed to start AI analysis. Please try again.')
            setPollingStartTime(null)
        }
    }

    const handlePageChange = (page: number) => {
        fetchSamples(page)
        window.scrollTo({ top: 0, behavior: 'smooth' })
    }

    const handleDownload = () => {
        if (dataset?.source?.url) {
            window.open(dataset.source.url, '_blank')
        }
    }

    const handleBack = () => {
        router.back()
    }

    const renderSampleValue = (value: any, key: string) => {
        if (value === null || value === undefined) {
            return <span className="text-gray-400 italic">null</span>
        }

        // Check if it's an audio object (array with src and type starting with 'audio/')
        if (Array.isArray(value) && value.length > 0 && value[0]?.src && value[0]?.type?.startsWith('audio/')) {
            const audioData = value[0]
            return (
                <div className="space-y-2">
                    <audio
                        controls
                        className="w-full max-w-md rounded-xl shadow-md"
                        preload="metadata"
                    >
                        <source src={audioData.src} type={audioData.type} />
                        Your browser does not support the audio element.
                    </audio>
                    <p className="text-xs text-gray-500 dark:text-gray-400">
                        {audioData.type} • Click to play
                    </p>
                </div>
            )
        }

        // Check if it's an image object (array with src, could be image type or just src)
        if (Array.isArray(value) && value.length > 0 && value[0]?.src) {
            const imageData = value[0]
            // Check if it's an image by type or by file extension
            const isImage = imageData.type?.startsWith('image/') ||
                imageData.src?.match(/\.(jpg|jpeg|png|gif|webp|bmp|svg)(\?|$)/i)

            if (isImage) {
                return (
                    <div className="space-y-2">
                        <img
                            src={imageData.src}
                            alt={key}
                            className="max-w-xs max-h-64 rounded-xl border-2 border-gray-200 dark:border-gray-700 shadow-md hover:shadow-xl transition-all cursor-pointer hover:scale-105"
                            onClick={() => {
                                const modal = document.getElementById(`image-modal-${key}`) as HTMLDialogElement
                                modal?.showModal()
                            }}
                        />
                        <button
                            className="text-primary-600 hover:text-primary-700 dark:text-primary-400 dark:hover:text-primary-300 text-sm font-medium hover:underline"
                            onClick={() => {
                                const modal = document.getElementById(`image-modal-${key}`) as HTMLDialogElement
                                modal?.showModal()
                            }}
                        >
                            View full size
                        </button>
                        <dialog id={`image-modal-${key}`} className="backdrop:bg-black/80 p-0 rounded-2xl max-w-6xl w-full shadow-2xl">
                            <div className="bg-white dark:bg-slate-800 p-6 rounded-2xl">
                                <div className="flex justify-between items-start mb-4">
                                    <h3 className="text-xl font-semibold text-gray-900 dark:text-white">{key}</h3>
                                    <button
                                        onClick={(e) => {
                                            const modal = e.currentTarget.closest('dialog') as HTMLDialogElement
                                            modal?.close()
                                        }}
                                        className="text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200 text-2xl font-bold w-8 h-8 flex items-center justify-center rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
                                    >
                                        ✕
                                    </button>
                                </div>
                                <img
                                    src={imageData.src}
                                    alt={key}
                                    className="w-full h-auto max-h-[80vh] object-contain rounded-xl"
                                />
                            </div>
                        </dialog>
                    </div>
                )
            }
        }

        // Check if it's an image (data URI or URL)
        if (typeof value === 'string') {
            if (value.startsWith('data:image')) {
                return (
                    <img
                        src={value}
                        alt={key}
                        className="max-w-xs max-h-48 rounded-xl border-2 border-gray-200 dark:border-gray-700 shadow-md hover:shadow-xl transition-shadow"
                    />
                )
            }
            if (value.length > 300) {
                return (
                    <div className="space-y-2">
                        <p className="text-gray-700 dark:text-gray-300 whitespace-pre-wrap">
                            {value.substring(0, 300)}...
                        </p>
                        <button
                            className="text-primary-600 hover:text-primary-700 dark:text-primary-400 dark:hover:text-primary-300 text-sm font-medium hover:underline"
                            onClick={() => {
                                const modal = document.getElementById(`modal-${key}`) as HTMLDialogElement
                                modal?.showModal()
                            }}
                        >
                            Show full text
                        </button>
                        <dialog id={`modal-${key}`} className="backdrop:bg-black/50 p-0 rounded-2xl max-w-4xl w-full shadow-2xl">
                            <div className="bg-white dark:bg-slate-800 p-8 rounded-2xl">
                                <div className="flex justify-between items-start mb-4">
                                    <h3 className="text-xl font-semibold text-gray-900 dark:text-white">{key}</h3>
                                    <button
                                        onClick={(e) => {
                                            const modal = e.currentTarget.closest('dialog') as HTMLDialogElement
                                            modal?.close()
                                        }}
                                        className="text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200 text-2xl font-bold w-8 h-8 flex items-center justify-center rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
                                    >
                                        ✕
                                    </button>
                                </div>
                                <p className="text-gray-700 dark:text-gray-300 whitespace-pre-wrap max-h-96 overflow-y-auto">
                                    {value}
                                </p>
                            </div>
                        </dialog>
                    </div>
                )
            }
            return <p className="text-gray-700 dark:text-gray-300 whitespace-pre-wrap">{value}</p>
        }

        if (typeof value === 'number' || typeof value === 'boolean') {
            return <span className="text-gray-700 dark:text-gray-300 font-mono">{String(value)}</span>
        }

        if (Array.isArray(value)) {
            return (
                <div className="space-y-1">
                    {value.slice(0, 5).map((item, idx) => (
                        <div key={idx} className="text-gray-700 dark:text-gray-300 font-mono text-sm">
                            {String(item)}
                        </div>
                    ))}
                    {value.length > 5 && (
                        <span className="text-gray-500 text-sm">... and {value.length - 5} more</span>
                    )}
                </div>
            )
        }

        // Check if it's a plain image object (not in an array) with src property
        if (typeof value === 'object' && !Array.isArray(value) && value?.src) {
            // Check if it's an image by type or by file extension
            const isImage = value.type?.startsWith('image/') ||
                value.src?.match(/\.(jpg|jpeg|png|gif|webp|bmp|svg)(\?|$)/i)

            if (isImage) {
                return (
                    <div className="space-y-2">
                        <img
                            src={value.src}
                            alt={key}
                            className="max-w-xs max-h-64 rounded-xl border-2 border-gray-200 dark:border-gray-700 shadow-md hover:shadow-xl transition-all cursor-pointer hover:scale-105"
                            onClick={() => {
                                const modal = document.getElementById(`image-modal-${key}`) as HTMLDialogElement
                                modal?.showModal()
                            }}
                        />
                        <button
                            className="text-primary-600 hover:text-primary-700 dark:text-primary-400 dark:hover:text-primary-300 text-sm font-medium hover:underline"
                            onClick={() => {
                                const modal = document.getElementById(`image-modal-${key}`) as HTMLDialogElement
                                modal?.showModal()
                            }}
                        >
                            View full size
                        </button>
                        <dialog id={`image-modal-${key}`} className="backdrop:bg-black/80 p-0 rounded-2xl max-w-6xl w-full shadow-2xl">
                            <div className="bg-white dark:bg-slate-800 p-6 rounded-2xl">
                                <div className="flex justify-between items-start mb-4">
                                    <h3 className="text-xl font-semibold text-gray-900 dark:text-white">{key}</h3>
                                    <button
                                        onClick={(e) => {
                                            const modal = e.currentTarget.closest('dialog') as HTMLDialogElement
                                            modal?.close()
                                        }}
                                        className="text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200 text-2xl font-bold w-8 h-8 flex items-center justify-center rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
                                    >
                                        ✕
                                    </button>
                                </div>
                                <img
                                    src={value.src}
                                    alt={key}
                                    className="w-full h-auto max-h-[80vh] object-contain rounded-xl"
                                />
                            </div>
                        </dialog>
                    </div>
                )
            }
        }

        if (typeof value === 'object') {
            return (
                <pre className="text-sm text-gray-700 dark:text-gray-300 bg-gray-50 dark:bg-gray-900 p-4 rounded-xl overflow-x-auto border border-gray-200 dark:border-gray-700">
                    {JSON.stringify(value, null, 2)}
                </pre>
            )
        }

        return <span className="text-gray-700 dark:text-gray-300">{String(value)}</span>
    }

    if (loading) {
        return (
            <div className="min-h-screen bg-gradient-to-br from-slate-50 via-blue-50 to-slate-100 dark:from-slate-900 dark:via-slate-800 dark:to-slate-900 flex items-center justify-center">
                <div className="text-center">
                    <div className="relative w-20 h-20 mx-auto mb-6">
                        <div className="absolute inset-0 rounded-full border-4 border-primary-200 dark:border-primary-900"></div>
                        <div className="absolute inset-0 rounded-full border-4 border-primary-600 dark:border-primary-400 border-t-transparent animate-spin"></div>
                    </div>
                    <p className="text-lg text-gray-600 dark:text-gray-400 font-medium">Loading dataset...</p>
                </div>
            </div>
        )
    }

    if (error || !dataset) {
        return (
            <div className="min-h-screen bg-gradient-to-br from-slate-50 via-blue-50 to-slate-100 dark:from-slate-900 dark:via-slate-800 dark:to-slate-900 flex items-center justify-center">
                <div className="text-center glass rounded-2xl p-12 max-w-md">
                    <div className="w-20 h-20 mx-auto mb-6 rounded-full bg-red-100 dark:bg-red-900/30 flex items-center justify-center">
                        <span className="text-4xl">⚠️</span>
                    </div>
                    <p className="text-red-600 dark:text-red-400 mb-6 text-lg font-medium">{error || 'Dataset not found'}</p>
                    <button
                        onClick={handleBack}
                        className="px-6 py-3 bg-gradient-to-r from-primary-500 to-primary-600 text-white rounded-xl hover:from-primary-600 hover:to-primary-700 transition-all duration-200 font-medium shadow-lg hover:shadow-xl hover:scale-105"
                    >
                        Back to Home
                    </button>
                </div>
            </div>
        )
    }

    return (
        <div className="min-h-screen bg-gradient-to-br from-slate-50 via-blue-50 to-slate-100 dark:from-slate-900 dark:via-slate-800 dark:to-slate-900">
            {/* Header */}
            <header className="bg-white dark:bg-gray-900 border-b border-gray-200 dark:border-gray-800 sticky top-0 z-40 shadow-sm">
                <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
                    <div className="flex items-center justify-between">
                        <div className="flex items-center gap-4">
                            <button
                                onClick={handleBack}
                                className="flex items-center gap-2 text-gray-600 dark:text-gray-400 hover:text-primary-600 dark:hover:text-primary-400 transition-colors group"
                            >
                                <ArrowLeftIcon className="h-5 w-5 group-hover:-translate-x-1 transition-transform" />
                                <span className="font-medium">Back</span>
                            </button>
                            <nav className="hidden md:flex items-center gap-6 ml-6 pl-6 border-l border-gray-200 dark:border-gray-700">
                                <a href="/" className="text-sm font-semibold text-gray-700 dark:text-gray-300 hover:text-primary-600 dark:hover:text-primary-400 transition-colors">
                                    Home
                                </a>
                                <a href="/explore" className="text-sm font-semibold text-gray-700 dark:text-gray-300 hover:text-primary-600 dark:hover:text-primary-400 transition-colors">
                                    Explore
                                </a>
                            </nav>
                        </div>
                        <DarkModeToggle />
                    </div>
                </div>
            </header>

            <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
                {/* Dataset Header Card */}
                <div className="relative glass rounded-2xl shadow-glass p-8 mb-8 overflow-hidden border border-gray-200 dark:border-gray-700 animate-scale-in">
                    <div className="absolute top-0 right-0 w-64 h-64 bg-gradient-to-br from-primary-500/10 to-purple-500/10 rounded-full blur-3xl"></div>

                    <div className="relative flex flex-col md:flex-row md:items-start md:justify-between gap-6">
                        <div className="flex-1">
                            <h1 className="text-4xl md:text-5xl font-bold text-gray-900 dark:text-white mb-4 break-words">
                                {dataset.name}
                            </h1>
                            <div className="flex flex-wrap gap-2 mb-4">
                                {dataset.domain && (
                                    <Badge variant="domain" size="lg">
                                        {dataset.domain}
                                    </Badge>
                                )}
                                {dataset.modality && (
                                    <Badge variant="modality" size="lg">
                                        {dataset.modality}
                                    </Badge>
                                )}
                                {dataset.license && (
                                    <Badge variant="license" size="lg">
                                        {dataset.license}
                                    </Badge>
                                )}
                            </div>
                        </div>

                        {/* Trust Signals */}
                        <TrustSignals
                            downloads={dataset.source?.source_metadata?.downloads}
                            likes={dataset.source?.source_metadata?.likes}
                            lastUpdated={dataset.source?.source_metadata?.last_modified}
                        />

                        <div className="flex gap-3 flex-wrap mt-6">
                            {dataset.source?.platform === 'huggingface' && (
                                <button
                                    onClick={() => {
                                        setActiveTab('samples')
                                        setShowSamples(true)
                                    }}
                                    className="flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-indigo-500 to-indigo-600 text-white rounded-xl hover:from-indigo-600 hover:to-indigo-700 transition-all duration-200 font-medium shadow-lg hover:shadow-xl hover:scale-105"
                                >
                                    <EyeIcon className="h-5 w-5" />
                                    View Samples
                                </button>
                            )}
                            {dataset.source?.url && (
                                <button
                                    onClick={handleDownload}
                                    className="flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-primary-500 to-primary-600 text-white rounded-xl hover:from-primary-600 hover:to-primary-700 transition-all duration-200 font-medium shadow-lg hover:shadow-xl hover:scale-105"
                                >
                                    <ArrowDownTrayIcon className="h-5 w-5" />
                                    Access Dataset
                                </button>
                            )}
                            {/* AI Analysis Button */}
                            <button
                                onClick={startAnalysis}
                                disabled={isAnalyzing}
                                className={`flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-purple-500 to-purple-600 text-white rounded-xl hover:from-purple-600 hover:to-purple-700 transition-all duration-200 font-medium shadow-lg hover:shadow-xl hover:scale-105 ${isAnalyzing ? 'opacity-50 cursor-not-allowed' : ''
                                    }`}
                            >
                                {isAnalyzing ? (
                                    <>
                                        <svg className="animate-spin h-5 w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                                            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                                            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                                        </svg>
                                        Analyzing...
                                    </>
                                ) : (
                                    <>
                                        <SparklesIcon className="h-5 w-5" />
                                        Analyze with AI
                                    </>
                                )}
                            </button>
                        </div>
                    </div>
                </div>

                {/* Dataset Summary Section */}
                <DatasetSummary
                    source={dataset.source?.platform}
                    domain={dataset.domain}
                    modality={dataset.modality}
                    rows={dataset.size?.samples}
                    license={dataset.license}
                    lastUpdated={dataset.source?.source_metadata?.last_modified}
                />



                {/* Tabs */}
                <div className="glass rounded-2xl shadow-glass mb-8 overflow-hidden border border-gray-200 dark:border-gray-700">
                    <div className="flex border-b border-gray-200 dark:border-gray-700 relative">
                        <button
                            onClick={() => setActiveTab('overview')}
                            className={`relative px-8 py-4 font-semibold transition-all duration-300 ${activeTab === 'overview'
                                ? 'text-primary-600 dark:text-primary-400'
                                : 'text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200'
                                }`}
                        >
                            Overview
                            {activeTab === 'overview' && (
                                <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-gradient-to-r from-primary-500 to-purple-500"></div>
                            )}
                        </button>
                        <button
                            onClick={() => {
                                setActiveTab('samples')
                                setShowSamples(true)
                            }}
                            className={`relative px-8 py-4 font-semibold transition-all duration-300 ${activeTab === 'samples'
                                ? 'text-primary-600 dark:text-primary-400'
                                : 'text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200'
                                }`}
                        >
                            Dataset Samples
                            {activeTab === 'samples' && (
                                <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-gradient-to-r from-primary-500 to-purple-500"></div>
                            )}
                        </button>
                        {/* AI Analysis tab - always visible */}
                        <button
                            onClick={() => setActiveTab('ai-analysis')}
                            className={`relative px-8 py-4 font-semibold transition-all duration-300 flex items-center gap-2 ${activeTab === 'ai-analysis'
                                ? 'text-primary-600 dark:text-primary-400'
                                : 'text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200'
                                }`}
                        >
                            <SparklesIcon className="h-5 w-5" />
                            AI Analysis
                            {activeTab === 'ai-analysis' && (
                                <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-gradient-to-r from-primary-500 to-purple-500"></div>
                            )}
                        </button>
                        {/* Intelligence tab - new features */}
                        <button
                            onClick={() => setActiveTab('intelligence')}
                            className={`relative px-8 py-4 font-semibold transition-all duration-300 flex items-center gap-2 ${activeTab === 'intelligence'
                                ? 'text-primary-600 dark:text-primary-400'
                                : 'text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200'
                                }`}
                        >
                            <ShieldCheckIcon className="h-5 w-5" />
                            Intelligence
                            {activeTab === 'intelligence' && (
                                <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-gradient-to-r from-primary-500 to-purple-500"></div>
                            )}
                        </button>
                    </div>
                </div>

                {activeTab === 'overview' && (
                    <>
                        {/* Stats Cards */}
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
                            {/* Source Card */}
                            <div className="glass rounded-2xl p-6 shadow-glass hover-lift border border-gray-200 dark:border-gray-700 group">
                                <div className="flex items-center gap-3 mb-4">
                                    <div className="p-3 rounded-xl bg-gradient-to-br from-primary-500 to-primary-600 shadow-lg group-hover:shadow-xl transition-shadow">
                                        <GlobeAltIcon className="h-6 w-6 text-white" />
                                    </div>
                                    <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Source</h3>
                                </div>
                                <div className="space-y-2">
                                    {dataset.source?.platform && (
                                        <p className="text-gray-600 dark:text-gray-400">
                                            <span className="font-medium text-gray-900 dark:text-white">Platform:</span>{' '}
                                            <span className="capitalize">{dataset.source.platform}</span>
                                        </p>
                                    )}
                                    {dataset.source?.platform_id && (
                                        <p className="text-gray-600 dark:text-gray-400 break-all text-sm">
                                            <span className="font-medium text-gray-900 dark:text-white">ID:</span> {dataset.source.platform_id}
                                        </p>
                                    )}
                                </div>
                            </div>

                            {/* Statistics Card */}
                            <div className="glass rounded-2xl p-6 shadow-glass hover-lift border border-gray-200 dark:border-gray-700 group">
                                <div className="flex items-center gap-3 mb-4">
                                    <div className="p-3 rounded-xl bg-gradient-to-br from-purple-500 to-purple-600 shadow-lg group-hover:shadow-xl transition-shadow">
                                        <ChartBarIcon className="h-6 w-6 text-white" />
                                    </div>
                                    <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Statistics</h3>
                                </div>
                                <div className="space-y-3">
                                    {dataset.source?.source_metadata?.downloads !== undefined && (
                                        <div className="flex items-center justify-between">
                                            <div className="flex items-center gap-2">
                                                <CloudArrowDownIcon className="h-5 w-5 text-blue-500" />
                                                <span className="text-gray-600 dark:text-gray-400">Downloads</span>
                                            </div>
                                            <span className="font-bold text-gray-900 dark:text-white">
                                                {dataset.source.source_metadata.downloads.toLocaleString()}
                                            </span>
                                        </div>
                                    )}
                                    {dataset.source?.source_metadata?.likes !== undefined && (
                                        <div className="flex items-center justify-between">
                                            <div className="flex items-center gap-2">
                                                <HeartIcon className="h-5 w-5 text-red-500" />
                                                <span className="text-gray-600 dark:text-gray-400">Likes</span>
                                            </div>
                                            <span className="font-bold text-gray-900 dark:text-white">
                                                {dataset.source.source_metadata.likes.toLocaleString()}
                                            </span>
                                        </div>
                                    )}
                                    {dataset.size?.samples && (
                                        <div className="flex items-center justify-between">
                                            <div className="flex items-center gap-2">
                                                <UserGroupIcon className="h-5 w-5 text-green-500" />
                                                <span className="text-gray-600 dark:text-gray-400">Samples</span>
                                            </div>
                                            <span className="font-bold text-gray-900 dark:text-white">
                                                {dataset.size.samples.toLocaleString()}
                                            </span>
                                        </div>
                                    )}
                                    {dataset.size?.file_size_gb && (
                                        <div className="flex items-center justify-between">
                                            <div className="flex items-center gap-2">
                                                <SparklesIcon className="h-5 w-5 text-yellow-500" />
                                                <span className="text-gray-600 dark:text-gray-400">Size</span>
                                            </div>
                                            <span className="font-bold text-gray-900 dark:text-white">
                                                {dataset.size.file_size_gb} GB
                                            </span>
                                        </div>
                                    )}
                                </div>
                            </div>

                            {/* Info Card */}
                            <div className="glass rounded-2xl p-6 shadow-glass hover-lift border border-gray-200 dark:border-gray-700 group">
                                <div className="flex items-center gap-3 mb-4">
                                    <div className="p-3 rounded-xl bg-gradient-to-br from-orange-500 to-orange-600 shadow-lg group-hover:shadow-xl transition-shadow">
                                        <CalendarIcon className="h-6 w-6 text-white" />
                                    </div>
                                    <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Info</h3>
                                </div>
                                <div className="space-y-2">
                                    {dataset.source?.source_metadata?.author && (
                                        <p className="text-gray-600 dark:text-gray-400">
                                            <span className="font-medium text-gray-900 dark:text-white">Author:</span>{' '}
                                            {dataset.source.source_metadata.author}
                                        </p>
                                    )}
                                    {dataset.source?.source_metadata?.last_modified && (
                                        <p className="text-gray-600 dark:text-gray-400">
                                            <span className="font-medium text-gray-900 dark:text-white">Updated:</span>{' '}
                                            {new Date(dataset.source.source_metadata.last_modified).toLocaleDateString()}
                                        </p>
                                    )}
                                </div>
                            </div>
                        </div>

                        {/* Description */}
                        <div className="glass rounded-2xl p-8 shadow-glass mb-8 border border-gray-200 dark:border-gray-700">
                            <h3 className="text-2xl font-bold text-gray-900 dark:text-white mb-4">Description</h3>
                            <p className="text-gray-700 dark:text-gray-300 whitespace-pre-wrap leading-relaxed">
                                {dataset.description || 'No description available'}
                            </p>
                        </div>

                        {/* Tags */}
                        {dataset.source?.source_metadata?.tags && dataset.source.source_metadata.tags.length > 0 && (
                            <div className="glass rounded-2xl p-8 shadow-glass border border-gray-200 dark:border-gray-700">
                                <div className="flex items-center gap-3 mb-6">
                                    <div className="p-3 rounded-xl bg-gradient-to-br from-pink-500 to-pink-600 shadow-lg">
                                        <TagIcon className="h-6 w-6 text-white" />
                                    </div>
                                    <h3 className="text-2xl font-bold text-gray-900 dark:text-white">Tags</h3>
                                </div>
                                <div className="flex flex-wrap gap-2">
                                    {dataset.source.source_metadata.tags.slice(0, 20).map((tag: string, index: number) => (
                                        <Badge key={index} variant="tag" size="md">
                                            {tag}
                                        </Badge>
                                    ))}
                                </div>
                            </div>
                        )}
                    </>
                )}

                {activeTab === 'samples' && (
                    <div className="glass rounded-2xl shadow-glass p-8 border border-gray-200 dark:border-gray-700">
                        {samplesLoading && (
                            <div className="flex items-center justify-center py-16">
                                <div className="relative w-16 h-16">
                                    <div className="absolute inset-0 rounded-full border-4 border-primary-200 dark:border-primary-900"></div>
                                    <div className="absolute inset-0 rounded-full border-4 border-primary-600 dark:border-primary-400 border-t-transparent animate-spin"></div>
                                </div>
                            </div>
                        )}

                        {samplesError && (
                            <div className="text-center py-16">
                                <div className="w-20 h-20 mx-auto mb-6 rounded-full bg-amber-100 dark:bg-amber-900/30 flex items-center justify-center">
                                    <span className="text-4xl">📊</span>
                                </div>
                                <h3 className="text-xl font-bold text-gray-900 dark:text-white mb-2">Data Samples Not Available</h3>
                                <p className="text-gray-600 dark:text-gray-400 mb-4 max-w-md mx-auto">
                                    {dataset.modality === 'image' || dataset.modality === 'audio' || dataset.modality === 'video' ? (
                                        <>This {dataset.modality} dataset doesn't have previewable samples. Visit the source to access the full dataset.</>
                                    ) : (
                                        <>Dataset samples are not available for preview. This could be due to the dataset format or source restrictions.</>
                                    )}
                                </p>
                                {dataset.source?.url && (
                                    <a
                                        href={dataset.source.url}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="inline-flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-primary-500 to-primary-600 text-white rounded-xl hover:from-primary-600 hover:to-primary-700 transition-all duration-200 font-medium shadow-lg hover:shadow-xl hover:scale-105"
                                    >
                                        Access Dataset Source
                                        <LinkIcon className="h-5 w-5" />
                                    </a>
                                )}
                            </div>
                        )}

                        {!samplesLoading && !samplesError && samples && samples.samples.length > 0 && (
                            <>
                                <div className="mb-8">
                                    <h2 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
                                        Dataset Samples
                                    </h2>
                                    <p className="text-gray-600 dark:text-gray-400">
                                        Showing {((currentPage - 1) * 10) + 1} - {Math.min(currentPage * 10, samples.pagination.total)} of {samples.pagination.total} rows
                                    </p>
                                </div>

                                <div className="space-y-6">
                                    {samples.samples.map((sample, index) => (
                                        <DatasetSampleCard
                                            key={sample.row_idx}
                                            sample={{ data: sample.row, row_idx: sample.row_idx }}
                                            index={(currentPage - 1) * 10 + index}
                                            onImageClick={setImageModalUrl}
                                        />
                                    ))}
                                </div>

                                {samples.pagination.pages > 1 && (
                                    <div className="mt-10 flex items-center justify-between border-t-2 border-gray-200 dark:border-gray-700 pt-8">
                                        <button
                                            onClick={() => handlePageChange(currentPage - 1)}
                                            disabled={currentPage === 1}
                                            className={`flex items-center gap-2 px-6 py-3 rounded-xl font-medium transition-all duration-200 ${currentPage === 1
                                                ? 'bg-gray-100 dark:bg-gray-800 text-gray-400 cursor-not-allowed'
                                                : 'bg-gradient-to-r from-primary-500 to-primary-600 text-white hover:from-primary-600 hover:to-primary-700 shadow-lg hover:shadow-xl hover:scale-105'
                                                }`}
                                        >
                                            <ChevronLeftIcon className="h-5 w-5" />
                                            Previous
                                        </button>

                                        <div className="flex items-center gap-2">
                                            {Array.from({ length: Math.min(5, samples.pagination.pages) }, (_, i) => {
                                                let pageNum
                                                if (samples.pagination.pages <= 5) {
                                                    pageNum = i + 1
                                                } else if (currentPage <= 3) {
                                                    pageNum = i + 1
                                                } else if (currentPage >= samples.pagination.pages - 2) {
                                                    pageNum = samples.pagination.pages - 4 + i
                                                } else {
                                                    pageNum = currentPage - 2 + i
                                                }

                                                return (
                                                    <button
                                                        key={pageNum}
                                                        onClick={() => handlePageChange(pageNum)}
                                                        className={`px-4 py-2 rounded-xl font-medium transition-all duration-200 ${currentPage === pageNum
                                                            ? 'bg-gradient-to-r from-primary-500 to-primary-600 text-white shadow-lg scale-110'
                                                            : 'bg-gray-100 dark:bg-gray-800 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-700 hover:scale-105'
                                                            }`}
                                                    >
                                                        {pageNum}
                                                    </button>
                                                )
                                            })}
                                        </div>

                                        <button
                                            onClick={() => handlePageChange(currentPage + 1)}
                                            disabled={currentPage === samples.pagination.pages}
                                            className={`flex items-center gap-2 px-6 py-3 rounded-xl font-medium transition-all duration-200 ${currentPage === samples.pagination.pages
                                                ? 'bg-gray-100 dark:bg-gray-800 text-gray-400 cursor-not-allowed'
                                                : 'bg-gradient-to-r from-primary-500 to-primary-600 text-white hover:from-primary-600 hover:to-primary-700 shadow-lg hover:shadow-xl hover:scale-105'
                                                }`}
                                        >
                                            Next
                                            <ChevronRightIcon className="h-5 w-5" />
                                        </button>
                                    </div>
                                )}
                            </>
                        )}

                        {/* Empty state when samples loaded but none available */}
                        {!samplesLoading && !samplesError && (!samples || samples.samples.length === 0) && (
                            <div className="text-center py-16">
                                <div className="w-20 h-20 mx-auto mb-6 rounded-full bg-amber-100 dark:bg-amber-900/30 flex items-center justify-center">
                                    <span className="text-4xl">📊</span>
                                </div>
                                <h3 className="text-xl font-bold text-gray-900 dark:text-white mb-2">Data Samples Not Available</h3>
                                <p className="text-gray-600 dark:text-gray-400 mb-4 max-w-md mx-auto">
                                    {dataset.modality === 'image' ? (
                                        <>This image dataset doesn't have inline sample previews. The dataset contains image files that can be accessed from the source.</>
                                    ) : dataset.modality === 'audio' ? (
                                        <>This audio dataset doesn't have inline sample previews. The dataset contains audio files that can be accessed from the source.</>
                                    ) : dataset.modality === 'video' ? (
                                        <>This video dataset doesn't have inline sample previews. The dataset contains video files that can be accessed from the source.</>
                                    ) : (
                                        <>Sample data preview is not available for this dataset. You can access the full dataset from the source.</>
                                    )}
                                </p>
                                {dataset.source?.url && (
                                    <a
                                        href={dataset.source.url}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="inline-flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-primary-500 to-primary-600 text-white rounded-xl hover:from-primary-600 hover:to-primary-700 transition-all duration-200 font-medium shadow-lg hover:shadow-xl hover:scale-105"
                                    >
                                        Access Dataset Source
                                        <LinkIcon className="h-5 w-5" />
                                    </a>
                                )}
                            </div>
                        )}
                    </div>
                )}

                {activeTab === 'ai-analysis' && (
                    <div className="space-y-6">
                        {dataset.intelligence ? (
                            <>
                                {/* AI-Generated Summary */}
                                {dataset.intelligence.summary && (
                                    <div className="glass rounded-2xl p-8 shadow-glass border border-gray-200 dark:border-gray-700">
                                        <div className="flex items-center gap-3 mb-4">
                                            <div className="p-3 rounded-xl bg-gradient-to-br from-purple-500 to-purple-600 shadow-lg">
                                                <SparklesIcon className="h-6 w-6 text-white" />
                                            </div>
                                            <h3 className="text-2xl font-bold text-gray-900 dark:text-white">AI-Generated Summary</h3>
                                        </div>
                                        <p className="text-gray-700 dark:text-gray-300 leading-relaxed text-lg">
                                            {dataset.intelligence.summary}
                                        </p>
                                    </div>
                                )}

                                {/* Tasks & Use Cases */}
                                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                                    {dataset.intelligence.tasks && dataset.intelligence.tasks.length > 0 && (
                                        <div className="glass rounded-2xl p-6 shadow-glass border border-gray-200 dark:border-gray-700">
                                            <h4 className="text-lg font-bold text-gray-900 dark:text-white mb-4">ML Tasks</h4>
                                            <div className="flex flex-wrap gap-2">
                                                {dataset.intelligence.tasks.map((task, idx) => (
                                                    <Badge key={idx} variant="status" size="md">
                                                        {task.replace(/_/g, ' ')}
                                                    </Badge>
                                                ))}
                                            </div>
                                        </div>
                                    )}

                                    {dataset.intelligence.use_cases && dataset.intelligence.use_cases.length > 0 && (
                                        <div className="glass rounded-2xl p-6 shadow-glass border border-gray-200 dark:border-gray-700">
                                            <h4 className="text-lg font-bold text-gray-900 dark:text-white mb-4">Use Cases</h4>
                                            <div className="flex flex-wrap gap-2">
                                                {dataset.intelligence.use_cases.map((useCase, idx) => (
                                                    <Badge key={idx} variant="modality" size="md">
                                                        {useCase}
                                                    </Badge>
                                                ))}
                                            </div>
                                        </div>
                                    )}
                                </div>

                                {/* Modalities & Domain */}
                                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                                    {dataset.intelligence.modalities && dataset.intelligence.modalities.length > 0 && (
                                        <div className="glass rounded-2xl p-6 shadow-glass border border-gray-200 dark:border-gray-700">
                                            <h4 className="text-lg font-bold text-gray-900 dark:text-white mb-4">Data Modalities</h4>
                                            <div className="flex flex-wrap gap-2">
                                                {dataset.intelligence.modalities.map((mod, idx) => (
                                                    <Badge key={idx} variant="domain" size="md">
                                                        {mod}
                                                    </Badge>
                                                ))}
                                            </div>
                                        </div>
                                    )}

                                    <div className="glass rounded-2xl p-6 shadow-glass border border-gray-200 dark:border-gray-700">
                                        <h4 className="text-lg font-bold text-gray-900 dark:text-white mb-4">Domain & Difficulty</h4>
                                        <div className="space-y-3">
                                            {dataset.intelligence.domain && (
                                                <div>
                                                    <span className="text-sm text-gray-600 dark:text-gray-400">Domain:</span>
                                                    <p className="text-gray-900 dark:text-white font-semibold">{dataset.intelligence.domain}</p>
                                                </div>
                                            )}
                                            {dataset.intelligence.difficulty && (
                                                <div>
                                                    <span className="text-sm text-gray-600 dark:text-gray-400">Difficulty:</span>
                                                    <div className="mt-1">
                                                        <Badge
                                                            variant={
                                                                dataset.intelligence.difficulty === 'easy' ? 'success' :
                                                                    dataset.intelligence.difficulty === 'hard' ? 'danger' :
                                                                        'warning'
                                                            }
                                                            size="md"
                                                        >
                                                            {dataset.intelligence.difficulty.toUpperCase()}
                                                        </Badge>
                                                    </div>
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                </div>

                                {/* Field Semantics */}
                                {dataset.intelligence.fields && dataset.intelligence.fields.length > 0 && (
                                    <div className="glass rounded-2xl p-8 shadow-glass border border-gray-200 dark:border-gray-700">
                                        <h3 className="text-2xl font-bold text-gray-900 dark:text-white mb-6">Field Semantics</h3>
                                        <div className="overflow-x-auto">
                                            <table className="w-full">
                                                <thead>
                                                    <tr className="border-b-2 border-gray-200 dark:border-gray-700">
                                                        <th className="text-left py-3 px-4 text-gray-700 dark:text-gray-300 font-semibold">Field Name</th>
                                                        <th className="text-left py-3 px-4 text-gray-700 dark:text-gray-300 font-semibold">Meaning</th>
                                                        <th className="text-left py-3 px-4 text-gray-700 dark:text-gray-300 font-semibold">Type</th>
                                                        <th className="text-left py-3 px-4 text-gray-700 dark:text-gray-300 font-semibold">Role</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    {dataset.intelligence.fields.map((field, idx) => (
                                                        <tr key={idx} className="border-b border-gray-100 dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-gray-800/50">
                                                            <td className="py-3 px-4 font-mono text-sm text-gray-900 dark:text-white">{field.name}</td>
                                                            <td className="py-3 px-4 text-gray-700 dark:text-gray-300">{field.semantic_meaning}</td>
                                                            <td className="py-3 px-4">
                                                                <Badge variant="tag" size="sm">{field.data_type}</Badge>
                                                            </td>
                                                            <td className="py-3 px-4">
                                                                <Badge variant="status" size="sm">{field.role}</Badge>
                                                            </td>
                                                        </tr>
                                                    ))}
                                                </tbody>
                                            </table>
                                        </div>
                                    </div>
                                )}

                                {/* Quality Notes */}
                                {dataset.intelligence.quality_notes && dataset.intelligence.quality_notes.length > 0 && (
                                    <div className="glass rounded-2xl p-6 shadow-glass border border-gray-200 dark:border-gray-700">
                                        <h4 className="text-lg font-bold text-gray-900 dark:text-white mb-4">Quality Signals</h4>
                                        <ul className="space-y-2">
                                            {dataset.intelligence.quality_notes.map((note, idx) => (
                                                <li key={idx} className="flex items-start gap-2 text-gray-700 dark:text-gray-300">
                                                    <span className="text-green-500 mt-1">✓</span>
                                                    <span>{note}</span>
                                                </li>
                                            ))}
                                        </ul>
                                    </div>
                                )}

                                {/* Ethical Flags */}
                                {dataset.intelligence.ethical_flags && dataset.intelligence.ethical_flags.length > 0 && (
                                    <div className="glass rounded-2xl p-6 shadow-glass border border-yellow-200 dark:border-yellow-800 bg-yellow-50/50 dark:bg-yellow-900/10">
                                        <h4 className="text-lg font-bold text-gray-900 dark:text-white mb-4 flex items-center gap-2">
                                            <span className="text-yellow-500">⚠️</span>
                                            Ethical Considerations
                                        </h4>
                                        <ul className="space-y-2">
                                            {dataset.intelligence.ethical_flags.map((flag, idx) => (
                                                <li key={idx} className="flex items-start gap-2 text-gray-700 dark:text-gray-300">
                                                    <span className="text-yellow-500 mt-1">•</span>
                                                    <span>{flag}</span>
                                                </li>
                                            ))}
                                        </ul>
                                    </div>
                                )}

                                {/* AI-Generated Tags */}
                                {dataset.intelligence.tags && dataset.intelligence.tags.length > 0 && (
                                    <div className="glass rounded-2xl p-8 shadow-glass border border-gray-200 dark:border-gray-700">
                                        <h3 className="text-2xl font-bold text-gray-900 dark:text-white mb-6">AI-Generated Tags</h3>
                                        <div className="flex flex-wrap gap-2">
                                            {dataset.intelligence.tags.map((tag, idx) => (
                                                <Badge key={idx} variant="tag" size="md">
                                                    {tag}
                                                </Badge>
                                            ))}
                                        </div>
                                    </div>
                                )}

                                {/* Analysis Metadata */}
                                {dataset.intelligence_updated_at && (
                                    <div className="text-center text-sm text-gray-500 dark:text-gray-400">
                                        <p>Analysis generated on {new Date(dataset.intelligence_updated_at).toLocaleDateString()} using AI</p>
                                    </div>
                                )}
                            </>
                        ) : (
                            /* Empty state - no intelligence yet */
                            <div className="glass rounded-2xl p-12 shadow-glass border border-gray-200 dark:border-gray-700 text-center">
                                <div className="max-w-md mx-auto">
                                    <div className="w-20 h-20 mx-auto mb-6 rounded-full bg-gradient-to-br from-purple-500 to-purple-600 flex items-center justify-center shadow-lg">
                                        <SparklesIcon className="h-10 w-10 text-white" />
                                    </div>
                                    <h3 className="text-2xl font-bold text-gray-900 dark:text-white mb-4">
                                        No AI Analysis Yet
                                    </h3>
                                    <p className="text-gray-600 dark:text-gray-400 mb-8 leading-relaxed">
                                        This dataset hasn't been analyzed by AI yet. Click the button below to generate comprehensive intelligence including summary, tasks, use cases, and more.
                                    </p>

                                    {/* Error message */}
                                    {analysisError && (
                                        <div className="mb-6 p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-xl">
                                            <p className="text-red-600 dark:text-red-400 text-sm">
                                                {analysisError}
                                            </p>
                                        </div>
                                    )}

                                    <button
                                        onClick={startAnalysis}
                                        disabled={isAnalyzing}
                                        className={`inline-flex items-center gap-2 px-8 py-4 bg-gradient-to-r from-purple-500 to-purple-600 text-white rounded-xl hover:from-purple-600 hover:to-purple-700 transition-all duration-200 font-semibold shadow-lg hover:shadow-xl hover:scale-105 ${isAnalyzing ? 'opacity-50 cursor-not-allowed' : ''
                                            }`}
                                    >
                                        {isAnalyzing ? (
                                            <>
                                                <svg className="animate-spin h-5 w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                                                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                                                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                                                </svg>
                                                Analyzing... (~15 seconds)
                                            </>
                                        ) : (
                                            <>
                                                <SparklesIcon className="h-6 w-6" />
                                                {analysisError ? 'Try Again' : 'Generate AI Analysis'}
                                            </>
                                        )}
                                    </button>
                                    {isAnalyzing && (
                                        <p className="mt-4 text-sm text-gray-500 dark:text-gray-400">
                                            AI is analyzing the dataset structure, content, and metadata...
                                        </p>
                                    )}
                                </div>
                            </div>
                        )}
                    </div>
                )}

                {/* Intelligence Tab Content */}
                {activeTab === 'intelligence' && (
                    <div className="space-y-6">
                        {/* Global Quality Index (GQI) */}
                        <GQIScore datasetId={dataset.id} />

                        {/* Quality Score & ML Recommendations */}
                        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                            <QualityScore datasetId={dataset.id} showBreakdown={true} />
                            <MLRecommendations datasetId={dataset.id} limit={5} />
                        </div>

                        {/* Fitness Score & License Safety */}
                        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                            <FitnessGauge datasetId={dataset.id} />
                            <LicenseBadge datasetId={dataset.id} />
                        </div>

                        {/* Model Recommendations & Bias Dashboard */}
                        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                            <ModelRecommendations datasetId={dataset.id} />
                            <BiasChart datasetId={dataset.id} />
                        </div>

                        {/* Version Timeline & Synthetic Suitability */}
                        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                            <VersionTimeline datasetId={dataset.id} />
                            <SyntheticSuitability datasetId={dataset.id} />
                        </div>

                        {/* Dataset Card Generator */}
                        <DatasetCardGen datasetId={dataset.id} />
                    </div>
                )}

                {/* Similar Datasets - Always visible at bottom */}
                <div className="mt-8">
                    <SimilarDatasets datasetId={dataset.id} />
                </div>
            </main>

            {/* Image Modal */}
            {imageModalUrl && (
                <div
                    className="fixed inset-0 z-50 bg-black/90 flex items-center justify-center p-4"
                    onClick={() => setImageModalUrl(null)}
                >
                    <button
                        onClick={() => setImageModalUrl(null)}
                        className="absolute top-4 right-4 p-2 bg-white/10 hover:bg-white/20 rounded-full transition-colors"
                    >
                        <XMarkIcon className="h-6 w-6 text-white" />
                    </button>
                    <img
                        src={imageModalUrl}
                        alt="Full size preview"
                        className="max-w-full max-h-full object-contain"
                        onClick={(e) => e.stopPropagation()}
                    />
                </div>
            )}
        </div>
    )
}
