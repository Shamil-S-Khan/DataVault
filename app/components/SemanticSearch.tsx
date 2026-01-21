'use client'

import { useState } from 'react'
import { MagnifyingGlassIcon, SparklesIcon, EyeIcon } from '@heroicons/react/24/outline'
import { useRouter } from 'next/navigation'
import Badge from './Badge'

interface SearchResult {
    id: string
    name: string
    canonical_name: string
    description: string
    domain?: string
    modality?: string
    similarity_score: number
    match_reasons?: string[]
    quality_label?: string
    source?: {
        platform?: string
    }
}

export default function SemanticSearch() {
    const [query, setQuery] = useState('')
    const [lastSearchQuery, setLastSearchQuery] = useState('')
    const [results, setResults] = useState<SearchResult[]>([])
    const [loading, setLoading] = useState(false)
    const [error, setError] = useState<string | null>(null)
    const [showResults, setShowResults] = useState(false)
    const [hasSearched, setHasSearched] = useState(false)
    const router = useRouter()

    const handleSearch = async (e: React.FormEvent) => {
        e.preventDefault()
        if (!query.trim()) return

        setLoading(true)
        setError(null)
        try {
            const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
            const params = new URLSearchParams({
                query: query.trim(),
                limit: '10'
            })
            const response = await fetch(
                `${apiUrl}/api/datasets/search/semantic?${params}`,
                { method: 'POST' }
            )
            if (!response.ok) throw new Error('Search failed')
            const data = await response.json()
            // New searches REPLACE old results
            setResults(data.results || [])
            setLastSearchQuery(query.trim())
            setShowResults(true)
            setHasSearched(true)
        } catch (err) {
            setError(err instanceof Error ? err.message : 'Search failed')
        } finally {
            setLoading(false)
        }
    }

    const handleToggleResults = () => {
        setShowResults(!showResults)
    }

    const handleResultClick = (id: string) => {
        setShowResults(false)
        router.push(`/dataset/${id}`)
    }

    return (
        <div className="relative">
            <form onSubmit={handleSearch} className="relative">
                <div className="relative group animate-fade-in">
                    <SparklesIcon className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-primary-500 group-focus-within:animate-pulse" />
                    <input
                        type="text"
                        value={query}
                        onChange={(e) => setQuery(e.target.value)}
                        placeholder="Try: medical imaging for pneumonia detection..."
                        className="w-full pl-12 pr-28 py-4 bg-white/70 dark:bg-gray-800/70 backdrop-blur-md border-2 border-gray-200 dark:border-gray-700 rounded-xl text-gray-900 dark:text-white placeholder-gray-400 focus:border-primary-500 dark:focus:border-primary-400 focus:ring-2 focus:ring-primary-200 dark:focus:ring-primary-900 focus:shadow-glow transition-all"
                    />
                    <div className="absolute right-2 top-1/2 -translate-y-1/2 flex items-center gap-2">
                        {/* View Results Button - Only show if has previous results */}
                        {hasSearched && results.length > 0 && (
                            <button
                                type="button"
                                onClick={handleToggleResults}
                                className="px-3 py-2 bg-gradient-to-r from-primary-500 to-secondary-500 hover:from-primary-600 hover:to-secondary-600 text-white rounded-lg font-semibold transition-all flex items-center gap-1.5 shadow-md hover:shadow-glow animate-scale-in"
                                title="View previous search results"
                            >
                                <EyeIcon className="w-4 h-4 hover:scale-110 transition-transform" />
                                <span className="text-xs">View</span>
                            </button>
                        )}
                        <button
                            type="submit"
                            disabled={loading || !query.trim()}
                            className="px-4 py-2 bg-primary-600 hover:bg-primary-700 disabled:bg-gray-300 dark:disabled:bg-gray-700 text-white rounded-lg font-semibold transition-all disabled:cursor-not-allowed hover:shadow-glow"
                        >
                            {loading ? (
                                <div className="w-5 h-5 border-2 border-white border-t-transparent rounded-full animate-spin" />
                            ) : (
                                <MagnifyingGlassIcon className="w-5 h-5" />
                            )}
                        </button>
                    </div>
                </div>
                <p className="mt-2 text-xs text-gray-500 dark:text-gray-400 flex items-center gap-1">
                    <SparklesIcon className="w-3 h-3" />
                    AI-powered semantic search understands meaning, not just keywords
                </p>
            </form>

            {/* Results Dropdown */}
            {showResults && (
                <div className="absolute top-full left-0 right-0 mt-2 bg-white/80 dark:bg-gray-800/80 backdrop-blur-xl rounded-2xl shadow-glass-lg border border-white/20 dark:border-gray-700/50 max-h-[600px] overflow-y-auto z-50 animate-slide-down">
                        {error ? (
                            <div className="p-6 text-center text-red-600 dark:text-red-400 bg-red-50/50 dark:bg-red-900/10 rounded-xl m-3">
                                <p className="font-semibold">{error}</p>
                            </div>
                        ) : results.length === 0 ? (
                            <div className="p-8 text-center">
                                <SparklesIcon className="w-12 h-12 text-gray-300 dark:text-gray-600 mx-auto mb-3 animate-pulse" />
                                <p className="text-gray-600 dark:text-gray-400 font-medium">
                                    No results found. Try a different query.
                                </p>
                                <p className="text-xs text-gray-500 dark:text-gray-500 mt-2">
                                    Tip: Run embeddings generation if you haven't already
                                </p>
                            </div>
                        ) : (
                            <>
                                <div className="p-4 border-b border-gray-200/50 dark:border-gray-700/50 bg-gradient-to-r from-primary-500/10 to-secondary-500/10 backdrop-blur-md">
                                    <div className="flex items-center justify-between">
                                        <p className="text-sm font-semibold text-gray-900 dark:text-white flex items-center gap-2">
                                            <SparklesIcon className="w-4 h-4 text-primary-500" />
                                            Found {results.length} semantically similar datasets
                                        </p>
                                        {lastSearchQuery && (
                                            <span className="text-xs text-gray-500 dark:text-gray-400 italic">
                                                "{lastSearchQuery}"
                                            </span>
                                        )}
                                    </div>
                                </div>
                                <div className="divide-y divide-gray-200/50 dark:divide-gray-700/50">
                                    {results.map((result, index) => (
                                        <button
                                            key={result.id}
                                            onClick={() => handleResultClick(result.id)}
                                            className="w-full p-5 hover:bg-gradient-to-r hover:from-primary-50/50 hover:to-secondary-50/50 dark:hover:from-primary-900/20 dark:hover:to-secondary-900/20 transition-all text-left group animate-fade-in"
                                            style={{ animationDelay: `${index * 50}ms` }}
                                        >
                                            <div className="flex items-start justify-between gap-3 mb-3">
                                                <div className="flex-1 min-w-0">
                                                    <div className="flex items-center gap-2 mb-1 flex-wrap">
                                                        <h4 className="font-semibold text-gray-900 dark:text-white text-sm group-hover:text-primary-600 dark:group-hover:text-primary-400 transition-colors">
                                                            {result.name}
                                                        </h4>
                                                        {result.source?.platform && (
                                                            <Badge variant="platform" size="xs">
                                                                {result.source.platform}
                                                            </Badge>
                                                        )}
                                                        {result.quality_label && (
                                                            <Badge 
                                                                variant={
                                                                    result.quality_label === 'Excellent' ? 'success' :
                                                                    result.quality_label === 'Good' ? 'primary' :
                                                                    result.quality_label === 'Fair' ? 'warning' : 'danger'
                                                                } 
                                                                size="xs"
                                                            >
                                                                {result.quality_label}
                                                            </Badge>
                                                        )}
                                                    </div>
                                                    <p className="text-xs text-gray-600 dark:text-gray-400 line-clamp-2 mb-2">
                                                        {result.description}
                                                    </p>
                                                    <div className="flex items-center gap-2 flex-wrap">
                                                        {result.domain && (
                                                            <Badge variant="domain" size="xs">
                                                                {result.domain}
                                                            </Badge>
                                                        )}
                                                        {result.modality && (
                                                            <Badge variant="modality" size="xs">
                                                                {result.modality}
                                                            </Badge>
                                                        )}
                                                    </div>
                                                    {/* Match Reasons */}
                                                    {result.match_reasons && result.match_reasons.length > 0 && (
                                                        <div className="mt-3 pt-3 border-t border-gray-100 dark:border-gray-800">
                                                            <p className="text-xs font-semibold text-gray-500 dark:text-gray-400 mb-1.5 flex items-center gap-1">
                                                                <SparklesIcon className="w-3 h-3" />
                                                                Why this matches:
                                                            </p>
                                                            <ul className="space-y-1">
                                                                {result.match_reasons.map((reason, idx) => (
                                                                    <li key={idx} className="text-xs text-gray-600 dark:text-gray-400 flex items-start gap-1.5">
                                                                        <span className="text-primary-500 mt-0.5">•</span>
                                                                        <span>{reason}</span>
                                                                    </li>
                                                                ))}
                                                            </ul>
                                                        </div>
                                                    )}
                                                </div>
                                                <div className="flex flex-col items-end gap-1 hover:scale-105 transition-transform">
                                                    <div className="px-3 py-1 bg-gradient-to-r from-primary-100 to-secondary-100 dark:from-primary-900/30 dark:to-secondary-900/30 rounded-full shadow-inner-glow">
                                                        <span className="text-xs font-bold text-primary-700 dark:text-primary-300">
                                                            {(result.similarity_score * 100).toFixed(0)}%
                                                        </span>
                                                    </div>
                                                    <span className="text-[10px] text-gray-400 dark:text-gray-500 font-medium">similarity</span>
                                                </div>
                                            </div>
                                        </button>
                                    ))}
                                </div>
                            </>
                        )}
                    </div>
                )}

            {/* Backdrop */}
            {showResults && (
                <div
                    className="fixed inset-0 z-40 bg-black/20 backdrop-blur-sm animate-fade-in"
                    onClick={() => setShowResults(false)}
                />
            )}
        </div>
    )
}
