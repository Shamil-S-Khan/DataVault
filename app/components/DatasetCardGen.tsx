'use client'

import { useEffect, useState } from 'react'
import { DocumentTextIcon, ArrowDownTrayIcon, ClipboardIcon, CheckIcon, ChevronDownIcon, ChevronUpIcon } from '@heroicons/react/24/outline'

interface CodeSnippet {
    language: string
    title: string
    code: string
}

interface CardSection {
    title: string
    [key: string]: any
}

interface DatasetCardGenProps {
    datasetId: string
}

export default function DatasetCardGen({ datasetId }: DatasetCardGenProps) {
    const [sections, setSections] = useState<Record<string, CardSection>>({})
    const [markdown, setMarkdown] = useState<string>('')
    const [loading, setLoading] = useState(true)
    const [expandedSections, setExpandedSections] = useState<Set<string>>(new Set(['overview']))
    const [copiedSnippet, setCopiedSnippet] = useState<string | null>(null)
    const [downloading, setDownloading] = useState(false)

    useEffect(() => {
        if (datasetId) {
            fetchCard()
        }
    }, [datasetId])

    const fetchCard = async () => {
        setLoading(true)
        try {
            const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
            const res = await fetch(`${apiUrl}/api/datasets/${datasetId}/card`)
            const data = await res.json()

            if (data.status === 'success') {
                setSections(data.sections || {})
                setMarkdown(data.markdown || '')
            }
        } catch (err) {
            console.error('Failed to load dataset card:', err)
        } finally {
            setLoading(false)
        }
    }

    const toggleSection = (section: string) => {
        const newExpanded = new Set(expandedSections)
        if (newExpanded.has(section)) {
            newExpanded.delete(section)
        } else {
            newExpanded.add(section)
        }
        setExpandedSections(newExpanded)
    }

    const copyToClipboard = async (text: string, id: string) => {
        try {
            await navigator.clipboard.writeText(text)
            setCopiedSnippet(id)
            setTimeout(() => setCopiedSnippet(null), 2000)
        } catch (err) {
            console.error('Failed to copy:', err)
        }
    }

    const downloadCard = async () => {
        setDownloading(true)
        try {
            const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
            const res = await fetch(`${apiUrl}/api/datasets/${datasetId}/card/download`)
            const blob = await res.blob()
            const url = window.URL.createObjectURL(blob)
            const a = document.createElement('a')
            a.href = url
            a.download = 'dataset_card.md'
            document.body.appendChild(a)
            a.click()
            a.remove()
            window.URL.revokeObjectURL(url)
        } catch (err) {
            console.error('Failed to download:', err)
        } finally {
            setDownloading(false)
        }
    }

    if (loading) {
        return (
            <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700">
                <h3 className="text-xl font-bold text-gray-900 dark:text-white mb-4">Dataset Card</h3>
                <div className="animate-pulse space-y-4">
                    <div className="h-20 bg-gray-200 dark:bg-gray-700 rounded-xl" />
                    <div className="h-32 bg-gray-200 dark:bg-gray-700 rounded-xl" />
                </div>
            </div>
        )
    }

    return (
        <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700">
            {/* Header */}
            <div className="flex items-start justify-between mb-6">
                <div>
                    <h3 className="text-xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                        <DocumentTextIcon className="w-6 h-6 text-blue-500" />
                        Dataset Card
                    </h3>
                    <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                        Auto-generated documentation
                    </p>
                </div>
                <button
                    onClick={downloadCard}
                    disabled={downloading}
                    className="flex items-center gap-2 px-4 py-2 bg-primary-500 hover:bg-primary-600 text-white rounded-lg transition-colors disabled:opacity-50"
                >
                    <ArrowDownTrayIcon className="w-5 h-5" />
                    {downloading ? 'Downloading...' : 'Download MD'}
                </button>
            </div>

            {/* Overview Section */}
            {sections.overview && (
                <div className="mb-4">
                    <button
                        onClick={() => toggleSection('overview')}
                        className="w-full flex items-center justify-between p-4 bg-gradient-to-r from-blue-50 to-purple-50 dark:from-blue-900/20 dark:to-purple-900/20 rounded-xl"
                    >
                        <span className="font-semibold text-gray-900 dark:text-white">
                            {sections.overview.title}
                        </span>
                        {expandedSections.has('overview') ? (
                            <ChevronUpIcon className="w-5 h-5 text-gray-500" />
                        ) : (
                            <ChevronDownIcon className="w-5 h-5 text-gray-500" />
                        )}
                    </button>

                    {expandedSections.has('overview') && (
                        <div className="p-4 border-x border-b border-gray-200 dark:border-gray-700 rounded-b-xl">
                            <p className="text-gray-700 dark:text-gray-300 mb-4">
                                {sections.overview.summary}
                            </p>
                            {sections.overview.highlights?.length > 0 && (
                                <div>
                                    <h4 className="font-medium text-gray-900 dark:text-white mb-2">Highlights</h4>
                                    <ul className="space-y-1">
                                        {sections.overview.highlights.map((h: string, i: number) => (
                                            <li key={i} className="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-400">
                                                <span className="w-1.5 h-1.5 rounded-full bg-primary-500" />
                                                {h}
                                            </li>
                                        ))}
                                    </ul>
                                </div>
                            )}
                        </div>
                    )}
                </div>
            )}

            {/* Dataset Info Section */}
            {sections.dataset_info && (
                <div className="mb-4">
                    <button
                        onClick={() => toggleSection('dataset_info')}
                        className="w-full flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-800/50 rounded-xl"
                    >
                        <span className="font-semibold text-gray-900 dark:text-white">
                            {sections.dataset_info.title}
                        </span>
                        {expandedSections.has('dataset_info') ? (
                            <ChevronUpIcon className="w-5 h-5 text-gray-500" />
                        ) : (
                            <ChevronDownIcon className="w-5 h-5 text-gray-500" />
                        )}
                    </button>

                    {expandedSections.has('dataset_info') && (
                        <div className="p-4 border-x border-b border-gray-200 dark:border-gray-700 rounded-b-xl">
                            <div className="grid grid-cols-2 gap-2 text-sm">
                                {Object.entries(sections.dataset_info.fields || {}).map(([key, value]) => (
                                    <div key={key} className="flex justify-between py-1 border-b border-gray-100 dark:border-gray-700">
                                        <span className="text-gray-500">{key}</span>
                                        <span className="text-gray-900 dark:text-white font-medium">{value as string}</span>
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}
                </div>
            )}

            {/* Usage Section */}
            {sections.usage && (
                <div className="mb-4">
                    <button
                        onClick={() => toggleSection('usage')}
                        className="w-full flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-800/50 rounded-xl"
                    >
                        <span className="font-semibold text-gray-900 dark:text-white">
                            {sections.usage.title}
                        </span>
                        {expandedSections.has('usage') ? (
                            <ChevronUpIcon className="w-5 h-5 text-gray-500" />
                        ) : (
                            <ChevronDownIcon className="w-5 h-5 text-gray-500" />
                        )}
                    </button>

                    {expandedSections.has('usage') && (
                        <div className="p-4 border-x border-b border-gray-200 dark:border-gray-700 rounded-b-xl space-y-4">
                            {/* Use Cases */}
                            {sections.usage.use_cases?.length > 0 && (
                                <div>
                                    <h4 className="font-medium text-gray-900 dark:text-white mb-2">Use Cases</h4>
                                    <div className="flex flex-wrap gap-2">
                                        {sections.usage.use_cases.map((uc: string, i: number) => (
                                            <span key={i} className="px-3 py-1 bg-primary-100 dark:bg-primary-900/30 text-primary-700 dark:text-primary-400 rounded-full text-sm">
                                                {uc}
                                            </span>
                                        ))}
                                    </div>
                                </div>
                            )}

                            {/* Code Snippets */}
                            {sections.usage.code_snippets?.map((snippet: CodeSnippet, i: number) => (
                                <div key={i}>
                                    <div className="flex items-center justify-between mb-2">
                                        <h4 className="font-medium text-gray-900 dark:text-white">{snippet.title}</h4>
                                        <button
                                            onClick={() => copyToClipboard(snippet.code, `snippet-${i}`)}
                                            className="flex items-center gap-1 text-sm text-gray-500 hover:text-primary-500"
                                        >
                                            {copiedSnippet === `snippet-${i}` ? (
                                                <>
                                                    <CheckIcon className="w-4 h-4 text-green-500" />
                                                    Copied!
                                                </>
                                            ) : (
                                                <>
                                                    <ClipboardIcon className="w-4 h-4" />
                                                    Copy
                                                </>
                                            )}
                                        </button>
                                    </div>
                                    <pre className="p-3 bg-gray-900 text-gray-100 rounded-lg overflow-x-auto text-sm">
                                        <code>{snippet.code}</code>
                                    </pre>
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            )}

            {/* Considerations Section */}
            {sections.considerations && (
                <div className="mb-4">
                    <button
                        onClick={() => toggleSection('considerations')}
                        className="w-full flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-800/50 rounded-xl"
                    >
                        <span className="font-semibold text-gray-900 dark:text-white">
                            {sections.considerations.title}
                        </span>
                        {expandedSections.has('considerations') ? (
                            <ChevronUpIcon className="w-5 h-5 text-gray-500" />
                        ) : (
                            <ChevronDownIcon className="w-5 h-5 text-gray-500" />
                        )}
                    </button>

                    {expandedSections.has('considerations') && (
                        <div className="p-4 border-x border-b border-gray-200 dark:border-gray-700 rounded-b-xl space-y-4">
                            {sections.considerations.ethical?.length > 0 && (
                                <div>
                                    <h4 className="font-medium text-yellow-600 dark:text-yellow-400 mb-2">⚠️ Ethical Considerations</h4>
                                    <ul className="space-y-1">
                                        {sections.considerations.ethical.map((e: string, i: number) => (
                                            <li key={i} className="text-sm text-gray-600 dark:text-gray-400 flex gap-2">
                                                <span>•</span>
                                                {e}
                                            </li>
                                        ))}
                                    </ul>
                                </div>
                            )}

                            {sections.considerations.limitations?.length > 0 && (
                                <div>
                                    <h4 className="font-medium text-orange-600 dark:text-orange-400 mb-2">📋 Limitations</h4>
                                    <ul className="space-y-1">
                                        {sections.considerations.limitations.map((l: string, i: number) => (
                                            <li key={i} className="text-sm text-gray-600 dark:text-gray-400 flex gap-2">
                                                <span>•</span>
                                                {l}
                                            </li>
                                        ))}
                                    </ul>
                                </div>
                            )}
                        </div>
                    )}
                </div>
            )}

            {/* Citation Section */}
            {sections.citation && (
                <div>
                    <button
                        onClick={() => toggleSection('citation')}
                        className="w-full flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-800/50 rounded-xl"
                    >
                        <span className="font-semibold text-gray-900 dark:text-white">
                            {sections.citation.title}
                        </span>
                        {expandedSections.has('citation') ? (
                            <ChevronUpIcon className="w-5 h-5 text-gray-500" />
                        ) : (
                            <ChevronDownIcon className="w-5 h-5 text-gray-500" />
                        )}
                    </button>

                    {expandedSections.has('citation') && (
                        <div className="p-4 border-x border-b border-gray-200 dark:border-gray-700 rounded-b-xl space-y-3">
                            <div>
                                <div className="flex items-center justify-between mb-2">
                                    <h4 className="font-medium text-gray-900 dark:text-white">BibTeX</h4>
                                    <button
                                        onClick={() => copyToClipboard(sections.citation.bibtex, 'bibtex')}
                                        className="flex items-center gap-1 text-sm text-gray-500 hover:text-primary-500"
                                    >
                                        {copiedSnippet === 'bibtex' ? (
                                            <>
                                                <CheckIcon className="w-4 h-4 text-green-500" />
                                                Copied!
                                            </>
                                        ) : (
                                            <>
                                                <ClipboardIcon className="w-4 h-4" />
                                                Copy
                                            </>
                                        )}
                                    </button>
                                </div>
                                <pre className="p-3 bg-gray-100 dark:bg-gray-800 rounded-lg text-xs overflow-x-auto">
                                    {sections.citation.bibtex}
                                </pre>
                            </div>
                        </div>
                    )}
                </div>
            )}
        </div>
    )
}
