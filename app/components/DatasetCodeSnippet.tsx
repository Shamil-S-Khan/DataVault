'use client'

import { useState, useEffect } from 'react'
import { CommandLineIcon, ClipboardIcon, CheckIcon, RocketLaunchIcon } from '@heroicons/react/24/outline'

interface SnippetData {
    dataset_id: string
    language: string
    platform: string
    snippet: string
    install_note: string
}

interface DatasetCodeSnippetProps {
    datasetId: string
}

export default function DatasetCodeSnippet({ datasetId }: DatasetCodeSnippetProps) {
    const [data, setData] = useState<SnippetData | null>(null)
    const [loading, setLoading] = useState(true)
    const [copied, setCopied] = useState(false)

    useEffect(() => {
        const fetchSnippet = async () => {
            try {
                const apiUrl = (process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001').replace(/\/api\/?$/, '')
                const response = await fetch(`${apiUrl}/api/datasets/${datasetId}/snippet?lang=python`)
                const result = await response.json()
                setData(result)
            } catch (err) {
                console.error('Failed to fetch snippet:', err)
            } finally {
                setLoading(false)
            }
        }

        fetchSnippet()
    }, [datasetId])

    const handleCopy = () => {
        if (!data?.snippet) return
        navigator.clipboard.writeText(data.snippet)
        setCopied(true)
        setTimeout(() => setCopied(false), 2000)
    }

    const openInColab = () => {
        if (!data?.snippet) return
        const encodedCode = encodeURIComponent(data.snippet)
        window.open(`https://colab.research.google.com/signup#code=${encodedCode}`, '_blank')
    }

    if (loading) {
        return (
            <div className="animate-pulse bg-gray-50 dark:bg-gray-900/50 rounded-2xl p-6 border border-gray-100 dark:border-gray-800">
                <div className="h-4 w-32 bg-gray-200 dark:bg-gray-700 rounded mb-4"></div>
                <div className="h-24 bg-gray-200 dark:bg-gray-700 rounded"></div>
            </div>
        )
    }

    if (!data?.snippet) return null

    return (
        <div className="bg-gray-900 rounded-2xl overflow-hidden border border-gray-800 shadow-2xl my-8 group">
            <div className="flex items-center justify-between px-6 py-3 bg-gray-800/50 border-b border-gray-800">
                <div className="flex items-center gap-2">
                    <CommandLineIcon className="h-5 w-5 text-primary-400" />
                    <span className="text-sm font-semibold text-gray-300">Load this dataset</span>
                    <span className="px-2 py-0.5 bg-gray-700 text-gray-400 text-[10px] font-bold rounded uppercase tracking-wider">
                        Python
                    </span>
                </div>
                <div className="flex items-center gap-3">
                    <button
                        onClick={openInColab}
                        className="flex items-center gap-1.5 text-xs font-medium text-gray-400 hover:text-white transition-colors"
                    >
                        <RocketLaunchIcon className="h-4 w-4" />
                        Open in Colab
                    </button>
                    <button
                        onClick={handleCopy}
                        className={`flex items-center gap-1.5 text-xs font-medium transition-colors ${copied ? 'text-emerald-400' : 'text-gray-400 hover:text-white'
                            }`}
                    >
                        {copied ? (
                            <>
                                <CheckIcon className="h-4 w-4" />
                                Copied
                            </>
                        ) : (
                            <>
                                <ClipboardIcon className="h-4 w-4" />
                                Copy
                            </>
                        )}
                    </button>
                </div>
            </div>
            <div className="p-6 relative">
                <pre className="text-sm font-mono text-gray-300 overflow-x-auto whitespace-pre">
                    <code>{data.snippet}</code>
                </pre>
                {data.install_note && (
                    <div className="mt-4 pt-4 border-t border-gray-800">
                        <p className="text-[11px] text-gray-500 font-medium italic">
                            💡 {data.install_note}
                        </p>
                    </div>
                )}
            </div>
        </div>
    )
}
