'use client'

import { useState, useEffect, useRef, useCallback } from 'react'
import { useRouter } from 'next/navigation'
import { MagnifyingGlassIcon, XMarkIcon, ArrowRightIcon, SparklesIcon } from '@heroicons/react/24/outline'

interface Suggestion {
    text: string
    description: string
}

interface ParsedToken {
    field: string
    operator: string
    value: string
}

interface SmartSearchProps {
    onSearch?: (query: string) => void
    placeholder?: string
    showExamples?: boolean
    className?: string
}

export default function SmartSearch({
    onSearch,
    placeholder = "Search datasets... (e.g., task:image-classification downloads>10000)",
    showExamples = true,
    className = ""
}: SmartSearchProps) {
    const [query, setQuery] = useState('')
    const [suggestions, setSuggestions] = useState<Suggestion[]>([])
    const [showSuggestions, setShowSuggestions] = useState(false)
    const [tokens, setTokens] = useState<ParsedToken[]>([])
    const [isValid, setIsValid] = useState(true)
    const [errors, setErrors] = useState<string[]>([])
    const [loading, setLoading] = useState(false)
    const inputRef = useRef<HTMLInputElement>(null)
    const router = useRouter()

    // Fetch suggestions as user types
    const fetchSuggestions = useCallback(async (partial: string) => {
        try {
            const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
            const response = await fetch(`${apiUrl}/api/datasets/search/suggestions?q=${encodeURIComponent(partial)}`)
            const data = await response.json()
            if (data.status === 'success') {
                setSuggestions(data.suggestions || [])
            }
        } catch (err) {
            // Silently fail suggestions
        }
    }, [])

    // Validate query
    const validateQuery = useCallback(async (q: string) => {
        if (!q.trim()) {
            setIsValid(true)
            setErrors([])
            setTokens([])
            return
        }

        try {
            const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
            const response = await fetch(`${apiUrl}/api/datasets/search/validate?q=${encodeURIComponent(q)}`)
            const data = await response.json()
            setIsValid(data.valid)
            setErrors(data.errors || [])
            setTokens(data.parsed_tokens || [])
        } catch (err) {
            // Silently fail validation
        }
    }, [])

    useEffect(() => {
        const debounceTimer = setTimeout(() => {
            fetchSuggestions(query)
            validateQuery(query)
        }, 300)

        return () => clearTimeout(debounceTimer)
    }, [query, fetchSuggestions, validateQuery])

    const handleSearch = async () => {
        if (!query.trim()) return

        setLoading(true)

        if (onSearch) {
            onSearch(query)
        } else {
            // Navigate to search results page
            router.push(`/search?q=${encodeURIComponent(query)}`)
        }

        setShowSuggestions(false)
        setLoading(false)
    }

    const handleKeyDown = (e: React.KeyboardEvent) => {
        if (e.key === 'Enter') {
            handleSearch()
        } else if (e.key === 'Escape') {
            setShowSuggestions(false)
        }
    }

    const handleSuggestionClick = (suggestion: Suggestion) => {
        // If suggestion is a complete field:value, append it
        if (query.trim() && !query.endsWith(' ')) {
            setQuery(suggestion.text)
        } else {
            setQuery((prev) => prev + suggestion.text)
        }
        setShowSuggestions(false)
        inputRef.current?.focus()
    }

    const clearQuery = () => {
        setQuery('')
        setTokens([])
        setErrors([])
        setIsValid(true)
        inputRef.current?.focus()
    }

    // Highlight syntax in query display
    const renderHighlightedQuery = () => {
        if (!query) return null

        const parts: JSX.Element[] = []
        let remaining = query
        let key = 0

        tokens.forEach(token => {
            const tokenText = `${token.field}${token.operator}${token.value}`
            const idx = remaining.indexOf(tokenText)

            if (idx > 0) {
                parts.push(
                    <span key={key++} className="text-gray-400">
                        {remaining.substring(0, idx)}
                    </span>
                )
            }

            parts.push(
                <span key={key++} className="bg-primary-100 dark:bg-primary-900/50 px-1 rounded text-primary-700 dark:text-primary-300">
                    <span className="font-medium">{token.field}</span>
                    <span className="text-gray-500">{token.operator}</span>
                    <span className="text-emerald-600 dark:text-emerald-400">{token.value}</span>
                </span>
            )

            remaining = remaining.substring(idx + tokenText.length)
        })

        if (remaining) {
            parts.push(
                <span key={key++} className="text-gray-600 dark:text-gray-400">
                    {remaining}
                </span>
            )
        }

        return parts.length > 0 ? (
            <div className="absolute inset-0 flex items-center px-12 pointer-events-none text-sm overflow-hidden">
                {parts}
            </div>
        ) : null
    }

    const exampleQueries = [
        { query: 'task:image-classification', label: 'Image Classification' },
        { query: 'modality:text license:mit', label: 'MIT Text Datasets' },
        { query: 'downloads>100000', label: 'Popular (>100K downloads)' },
        { query: 'domain:medical modality:image', label: 'Medical Images' },
    ]

    return (
        <div className={`relative ${className}`}>
            {/* Search Input */}
            <div className={`relative ${!isValid ? 'ring-2 ring-red-500' : ''} rounded-xl`}>
                <div className="relative">
                    <MagnifyingGlassIcon className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-400" />

                    <input
                        ref={inputRef}
                        type="text"
                        value={query}
                        onChange={(e) => setQuery(e.target.value)}
                        onFocus={() => setShowSuggestions(true)}
                        onBlur={() => setTimeout(() => setShowSuggestions(false), 200)}
                        onKeyDown={handleKeyDown}
                        placeholder={placeholder}
                        className="w-full pl-12 pr-24 py-4 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl text-gray-900 dark:text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent text-sm"
                    />

                    <div className="absolute right-2 top-1/2 -translate-y-1/2 flex items-center gap-2">
                        {query && (
                            <button
                                onClick={clearQuery}
                                className="p-1.5 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
                            >
                                <XMarkIcon className="w-5 h-5" />
                            </button>
                        )}
                        <button
                            onClick={handleSearch}
                            disabled={loading || !isValid}
                            className="px-4 py-2 bg-primary-500 text-white rounded-lg hover:bg-primary-600 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2 text-sm font-medium"
                        >
                            {loading ? (
                                <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
                            ) : (
                                <>
                                    Search
                                    <ArrowRightIcon className="w-4 h-4" />
                                </>
                            )}
                        </button>
                    </div>
                </div>
            </div>

            {/* Validation Errors */}
            {errors.length > 0 && (
                <div className="mt-2 text-sm text-red-500">
                    {errors.map((error, i) => (
                        <p key={i}>⚠️ {error}</p>
                    ))}
                </div>
            )}

            {/* Parsed Tokens Display */}
            {tokens.length > 0 && (
                <div className="mt-2 flex items-center gap-2 flex-wrap">
                    <span className="text-xs text-gray-500 dark:text-gray-400">Filters:</span>
                    {tokens.map((token, i) => (
                        <span
                            key={i}
                            className="inline-flex items-center gap-1 px-2 py-1 bg-primary-100 dark:bg-primary-900/30 text-primary-700 dark:text-primary-400 rounded-full text-xs"
                        >
                            <span className="font-medium">{token.field}</span>
                            <span className="text-gray-500">{token.operator}</span>
                            <span>{token.value}</span>
                        </span>
                    ))}
                </div>
            )}

            {/* Suggestions Dropdown */}
            {showSuggestions && suggestions.length > 0 && (
                <div className="absolute z-50 w-full mt-2 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl shadow-xl overflow-hidden">
                    <div className="p-2 border-b border-gray-100 dark:border-gray-700">
                        <p className="text-xs text-gray-500 dark:text-gray-400 flex items-center gap-1">
                            <SparklesIcon className="w-3 h-3" />
                            Suggestions
                        </p>
                    </div>
                    <ul>
                        {suggestions.map((suggestion, i) => (
                            <li key={i}>
                                <button
                                    onClick={() => handleSuggestionClick(suggestion)}
                                    className="w-full px-4 py-3 text-left hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors"
                                >
                                    <p className="text-sm font-medium text-gray-900 dark:text-white font-mono">
                                        {suggestion.text}
                                    </p>
                                    <p className="text-xs text-gray-500 dark:text-gray-400">
                                        {suggestion.description}
                                    </p>
                                </button>
                            </li>
                        ))}
                    </ul>
                </div>
            )}

            {/* Example Queries */}
            {showExamples && !query && (
                <div className="mt-4">
                    <p className="text-xs text-gray-500 dark:text-gray-400 mb-2">Try these queries:</p>
                    <div className="flex flex-wrap gap-2">
                        {exampleQueries.map((example, i) => (
                            <button
                                key={i}
                                onClick={() => {
                                    setQuery(example.query)
                                    inputRef.current?.focus()
                                }}
                                className="px-3 py-1.5 bg-gray-100 dark:bg-gray-800 hover:bg-gray-200 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-full text-xs transition-colors"
                            >
                                <span className="font-mono">{example.query}</span>
                                <span className="text-gray-500 ml-1">• {example.label}</span>
                            </button>
                        ))}
                    </div>
                </div>
            )}
        </div>
    )
}
