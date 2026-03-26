'use client'

import { useState, useEffect, useRef } from 'react'
import { PaperAirplaneIcon, ChatBubbleLeftRightIcon, SparklesIcon, ArrowPathIcon } from '@heroicons/react/24/outline'

interface Message {
    role: 'user' | 'assistant'
    content: string
}

interface DatasetChatProps {
    datasetId: string
    datasetName: string
    isProUser: boolean
}

export default function DatasetChat({ datasetId, datasetName, isProUser }: DatasetChatProps) {
    const [history, setHistory] = useState<Message[]>([])
    const [inputValue, setInputValue] = useState('')
    const [isLoading, setIsLoading] = useState(false)
    const [error, setError] = useState<string | null>(null)
    const [rateLimitExceeded, setRateLimitExceeded] = useState(false)

    const messagesEndRef = useRef<HTMLDivElement>(null)

    const scrollToBottom = () => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
    }

    useEffect(() => {
        scrollToBottom()
    }, [history, isLoading])

    const starterPrompts = [
        "Is this suitable for NLP classification?",
        "What's the label distribution like?",
        "How does the quality score break down?"
    ]

    const parseMarkdown = (text: string) => {
        // Simple manual parsing for bold and inline code
        return text
            .split('\n').map((line, i) => {
                // Bold
                let processed = line.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
                // Inline code
                processed = processed.replace(/`(.*?)`/g, '<code class="bg-gray-100 dark:bg-gray-800 px-1 rounded font-mono text-sm">$1</code>')
                // Bullet points
                if (processed.startsWith('- ')) {
                    return `<li class="ml-4 list-disc">${processed.substring(2)}</li>`
                }
                return `<p class="mb-2">${processed}</p>`
            }).join('')
    }

    const sendMessage = async (message: string) => {
        if (!message.trim() || isLoading) return

        const newUserMessage: Message = { role: 'user', content: message }
        const newHistory = [...history, newUserMessage].slice(-10)

        setHistory(prev => [...prev, newUserMessage])
        setInputValue('')
        setIsLoading(true)
        setError(null)
        setRateLimitExceeded(false)

        try {
            const apiUrl = (process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001').replace(/\/api\/?$/, '')
            const response = await fetch(`${apiUrl}/api/datasets/${datasetId}/chat`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    message: message,
                    history: newHistory.slice(0, -1) // Send previous history
                })
            })

            if (response.status === 429) {
                setRateLimitExceeded(true)
                return
            }

            if (!response.ok) {
                throw new Error('Failed to get response')
            }

            const data = await response.json()
            setHistory(prev => [...prev, { role: 'assistant', content: data.reply }])
        } catch (err) {
            setError('Something went wrong. Try again.')
        } finally {
            setIsLoading(false)
        }
    }

    const handleSubmit = (e: React.FormEvent) => {
        e.preventDefault()
        sendMessage(inputValue)
    }

    return (
        <div className="flex flex-col h-[600px] glass rounded-2xl border border-gray-200 dark:border-gray-700 overflow-hidden shadow-glass" aria-label={`Chat about ${datasetName}`}>
            {/* Header */}
            <div className="p-4 border-b border-gray-200 dark:border-gray-700 bg-white/50 dark:bg-gray-900/50 flex items-center justify-between">
                <div className="flex items-center gap-2">
                    <ChatBubbleLeftRightIcon className="h-5 w-5 text-primary-600 dark:text-primary-400" />
                    <h3 className="font-bold text-gray-900 dark:text-white">Dataset Assistant</h3>
                </div>
                <div className="flex items-center gap-1">
                    <SparklesIcon className="h-4 w-4 text-purple-500 animate-pulse" />
                    <span className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">AI Powered</span>
                </div>
            </div>

            {/* Message History Area */}
            <div className="flex-grow overflow-y-auto p-4 space-y-4 bg-slate-50/30 dark:bg-slate-900/10 custom-scrollbar">
                {history.length === 0 && !isLoading && (
                    <div className="h-full flex flex-col items-center justify-center text-center p-8">
                        <div className="w-16 h-16 bg-primary-100 dark:bg-primary-900/30 rounded-full flex items-center justify-center mb-4 transition-transform hover:scale-110">
                            <ChatBubbleLeftRightIcon className="h-8 w-8 text-primary-600 dark:text-primary-400" />
                        </div>
                        <h4 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">Ask anything about this dataset</h4>
                        <p className="text-sm text-gray-500 dark:text-gray-400 max-w-xs">
                            Get insights on quality, schema, suitability, or specific data points instantly.
                        </p>
                    </div>
                )}

                {history.map((msg, idx) => (
                    <div key={idx} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'} animate-fade-in`}>
                        <div
                            className={`max-w-[85%] p-3 px-4 shadow-sm ${msg.role === 'user'
                                    ? 'bg-primary-600/10 dark:bg-primary-400/10 text-gray-900 dark:text-white border border-primary-500/20 rounded-[12px_12px_2px_12px]'
                                    : 'bg-white dark:bg-gray-800 text-gray-900 dark:text-white border border-gray-200 dark:border-gray-700 rounded-[12px_12px_12px_2px]'
                                }`}
                        >
                            <div
                                className="text-sm leading-relaxed whitespace-pre-wrap select-text"
                                dangerouslySetInnerHTML={{ __html: parseMarkdown(msg.content) }}
                            />
                        </div>
                    </div>
                ))}

                {isLoading && (
                    <div className="flex justify-start animate-fade-in">
                        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-[12px_12px_12px_2px] p-3 px-4 shadow-sm">
                            <div className="flex gap-1.5 pt-1.5 pb-1 px-1">
                                <div className="w-1.5 h-1.5 rounded-full bg-gray-400 dark:bg-gray-500 animate-bounce"></div>
                                <div className="w-1.5 h-1.5 rounded-full bg-gray-400 dark:bg-gray-500 animate-bounce [animation-delay:0.2s]"></div>
                                <div className="w-1.5 h-1.5 rounded-full bg-gray-400 dark:bg-gray-500 animate-bounce [animation-delay:0.4s]"></div>
                            </div>
                        </div>
                    </div>
                )}

                {error && (
                    <div className="flex justify-center p-2">
                        <div className="flex items-center gap-2 bg-red-50 dark:bg-red-900/20 text-red-600 dark:text-red-400 px-3 py-1.5 rounded-full text-xs font-medium border border-red-200 dark:border-red-800">
                            <span>{error}</span>
                            <button onClick={() => sendMessage(history[history.length - 1].content)} className="hover:underline flex items-center gap-1">
                                <ArrowPathIcon className="h-3 w-3" /> Retry
                            </button>
                        </div>
                    </div>
                )}

                {rateLimitExceeded && (
                    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/20 backdrop-blur-sm animate-fade-in rounded-2xl">
                        <div className="bg-white dark:bg-gray-900 rounded-2xl p-6 max-w-sm w-full shadow-2xl border border-gray-200 dark:border-gray-800 text-center">
                            <div className="w-12 h-12 bg-yellow-100 dark:bg-yellow-900/30 rounded-full flex items-center justify-center mx-auto mb-4">
                                <SparklesIcon className="h-6 w-6 text-yellow-600 dark:text-yellow-400" />
                            </div>
                            <h4 className="text-lg font-bold text-gray-900 dark:text-white mb-2">Daily Limit Reached</h4>
                            <p className="text-sm text-gray-600 dark:text-gray-400 mb-6">
                                You've reached the free chat limit for today. Upgrade to Pro for unlimited dataset chat.
                            </p>
                            <div className="flex flex-col gap-2">
                                <a href="/pricing" className="bg-gradient-to-r from-primary-500 to-primary-600 text-white py-2.5 rounded-xl font-bold hover:shadow-lg transition-all">
                                    Upgrade to Pro
                                </a>
                                <button onClick={() => setRateLimitExceeded(false)} className="text-gray-500 text-sm font-medium hover:text-gray-700 dark:hover:text-gray-300 py-2">
                                    Maybe later
                                </button>
                            </div>
                        </div>
                    </div>
                )}

                <div ref={messagesEndRef} />
            </div>

            {/* Input Area */}
            <div className="p-4 bg-white dark:bg-gray-900 border-t border-gray-200 dark:border-gray-700">
                {history.length === 0 && !isLoading && (
                    <div className="flex flex-wrap gap-2 mb-4 animate-fade-in-up">
                        {starterPrompts.map((prompt, i) => (
                            <button
                                key={i}
                                onClick={() => sendMessage(prompt)}
                                className="text-xs bg-primary-50 dark:bg-primary-900/20 text-primary-700 dark:text-primary-300 px-3 py-1.5 rounded-full border border-primary-100 dark:border-primary-800/50 hover:bg-primary-100 dark:hover:bg-primary-900/40 transition-colors font-medium"
                            >
                                {prompt}
                            </button>
                        ))}
                    </div>
                )}

                <form onSubmit={handleSubmit} className="flex gap-2">
                    <input
                        type="text"
                        value={inputValue}
                        onChange={(e) => setInputValue(e.target.value)}
                        placeholder="Type your question..."
                        disabled={isLoading || rateLimitExceeded}
                        className="flex-grow bg-slate-50 dark:bg-slate-800 border border-gray-200 dark:border-gray-700 rounded-xl px-4 py-2.5 text-sm text-gray-900 dark:text-white placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-primary-500/50 transition-all disabled:opacity-50"
                    />
                    <button
                        type="submit"
                        disabled={!inputValue.trim() || isLoading || rateLimitExceeded}
                        className="bg-primary-600 hover:bg-primary-700 disabled:bg-gray-300 dark:disabled:bg-gray-800 text-white p-2.5 rounded-xl transition-all shadow-md hover:shadow-lg active:scale-95 group"
                    >
                        <PaperAirplaneIcon className={`h-5 w-5 ${inputValue.trim() ? 'group-hover:translate-x-0.5 group-hover:-translate-y-0.5' : ''} transition-transform`} />
                    </button>
                </form>
            </div>
        </div>
    )
}
