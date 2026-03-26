'use client'

import { useState, useEffect, useRef, useCallback } from 'react'
import { MagnifyingGlassIcon, XMarkIcon, ArrowRightIcon, SparklesIcon } from '@heroicons/react/24/outline'

interface SmartSearchProps {
    onSearch?: (query: string) => void
    onResults?: (results: any[], intent: any, total: number) => void
    placeholder?: string
    showExamples?: boolean
    className?: string
    filters?: {
        domain?: string
        modality?: string
        platform?: string
    }
    setLoading?: (loading: boolean) => void
}

export default function SmartSearch({
    onSearch,
    onResults,
    placeholder = "Search datasets in natural language (e.g., 'Popular medical image datasets')",
    showExamples = true,
    className = "",
    filters = {},
    setLoading: setParentLoading
}: SmartSearchProps) {
    const [query, setQuery] = useState('')
    const [intent, setIntent] = useState<any>(null)
    const [loading, setLoading] = useState(false)
    const [error, setError] = useState<string | null>(null)
    const inputRef = useRef<HTMLInputElement>(null)

    // Debounced intent extraction and search
    // In this version, we perform the full search on every significant change or Enter
    const performSearch = useCallback(async (q: string) => {
        if (!q.trim()) {
            setIntent(null)
            if (onResults) onResults([], null, 0)
            return
        }

        setLoading(true)
        if (setParentLoading) setParentLoading(true)
        setError(null)

        try {
            const apiUrl = (process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001').replace(/\/api\/?$/, '')
            const response = await fetch(`${apiUrl}/api/datasets/smart-search`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    query: q,
                    filters: {
                        domain: filters.domain || undefined,
                        modality: filters.modality || undefined,
                        platform: filters.platform || undefined
                    },
                    limit: 100,
                    offset: 0
                })
            })

            const data = await response.json()
            
            setIntent(data.intent)
            if (onResults) {
                onResults(data.results || [], data.intent, data.total || 0)
            }
            if (onSearch) {
                onSearch(q)
            }
        } catch (err) {
            console.error('Smart search failed:', err)
            setError('Search service is temporarily unavailable.')
        } finally {
            setLoading(false)
            if (setParentLoading) setParentLoading(false)
        }
    }, [onResults, onSearch, filters])

    useEffect(() => {
        const debounceTimer = setTimeout(() => {
            if (query.length > 3) {
                performSearch(query)
            }
        }, 600)

        return () => clearTimeout(debounceTimer)
    }, [query, performSearch])

    const handleSearch = () => {
        performSearch(query)
    }

    const handleKeyDown = (e: React.KeyboardEvent) => {
        if (e.key === 'Enter') {
            handleSearch()
        }
    }

    const clearQuery = () => {
        setQuery('')
        setIntent(null)
        if (onResults) onResults([], null, 0)
        inputRef.current?.focus()
    }

    const exampleQueries = [
        "Popular computer vision datasets for object detection",
        "NLP datasets with MIT license",
        "Large healthcare tabular datasets",
        "Audio datasets for speech recognition from HuggingFace",
    ]

    return (
        <div className={`relative ${className}`}>
            {/* Search Input */}
            <div className={`relative bg-white dark:bg-gray-800 rounded-2xl shadow-xl transition-all duration-300 border border-transparent focus-within:border-primary-500/50 focus-within:ring-4 focus-within:ring-primary-500/10`}>
                <div className="relative flex items-center p-1">
                    <div className="pl-4 text-gray-400">
                        <MagnifyingGlassIcon className="w-6 h-6" />
                    </div>

                    <input
                        ref={inputRef}
                        type="text"
                        value={query}
                        onChange={(e) => setQuery(e.target.value)}
                        onKeyDown={handleKeyDown}
                        placeholder={placeholder}
                        className="w-full px-4 py-4 bg-transparent border-none text-gray-900 dark:text-white placeholder-gray-400 focus:outline-none text-base md:text-lg rounded-xl"
                    />

                    <div className="flex items-center gap-2 pr-2">
                        {query && (
                            <button
                                onClick={clearQuery}
                                className="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 transition-colors"
                            >
                                <XMarkIcon className="w-5 h-5" />
                            </button>
                        )}
                        <button
                            onClick={handleSearch}
                            disabled={loading || !query.trim()}
                            className="px-6 py-3 bg-gradient-to-r from-primary-500 to-primary-600 text-white rounded-xl hover:shadow-lg hover:shadow-primary-500/20 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2 font-bold transition-all"
                        >
                            {loading ? (
                                <div className="w-5 h-5 border-2 border-white border-t-transparent rounded-full animate-spin" />
                            ) : (
                                <>
                                    Search
                                    <ArrowRightIcon className="w-5 h-5" />
                                </>
                            )}
                        </button>
                    </div>
                </div>
            </div>

            {/* Error Message */}
            {error && (
                <div className="mt-3 px-4 py-3 bg-red-50 dark:bg-red-900/20 border border-red-100 dark:border-red-900/30 rounded-xl text-sm text-red-600 dark:text-red-400 animate-fade-in">
                    ⚠️ {error}
                </div>
            )}

            {/* Intent Interpretation Line */}
            {intent && query && !loading && (
                <div className="mt-4 px-5 py-3 bg-primary-50/50 dark:bg-primary-900/10 backdrop-blur-sm rounded-xl border border-primary-100/50 dark:border-primary-900/20 animate-fade-in-up">
                    <p className="text-xs font-medium text-gray-500 dark:text-gray-400 flex items-center gap-2">
                        <SparklesIcon className="w-4 h-4 text-primary-500" />
                        AI interpreted as:
                        <span className="text-gray-900 dark:text-gray-200 font-semibold italic">
                            {intent.semantic_query || query}
                        </span>
                        {intent.domain && (
                            <span className="bg-primary-100 dark:bg-primary-900/40 text-primary-700 dark:text-primary-300 px-2 py-0.5 rounded-full text-[10px] uppercase tracking-wider">
                                {intent.domain}
                            </span>
                        )}
                        {intent.modality && (
                            <span className="bg-emerald-100 dark:bg-emerald-900/40 text-emerald-700 dark:text-emerald-300 px-2 py-0.5 rounded-full text-[10px] uppercase tracking-wider">
                                {intent.modality}
                            </span>
                        )}
                        {intent.min_samples && (
                            <span className="text-gray-500 dark:text-gray-400">
                                • {intent.min_samples.toLocaleString()}+ rows
                            </span>
                        )}
                    </p>
                </div>
            )}

            {/* Example Queries */}
            {showExamples && !query && (
                <div className="mt-6 px-2 animate-fade-in">
                    <h4 className="text-xs font-bold text-gray-400 dark:text-gray-500 uppercase tracking-widest mb-3 flex items-center gap-2">
                        <SparklesIcon className="w-4 h-4" />
                        Try AI Search
                    </h4>
                    <div className="flex flex-wrap gap-2">
                        {exampleQueries.map((example, i) => (
                            <button
                                key={i}
                                onClick={() => {
                                    setQuery(example)
                                    inputRef.current?.focus()
                                }}
                                className="px-4 py-2 bg-white dark:bg-gray-800/50 border border-gray-100 dark:border-gray-700 hover:border-primary-300 dark:hover:border-primary-700 hover:shadow-md text-gray-600 dark:text-gray-400 rounded-xl text-sm transition-all duration-300 text-left"
                            >
                                "{example}"
                            </button>
                        ))}
                    </div>
                </div>
            )}
        </div>
    )
}

