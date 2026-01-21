'use client'

import { useState } from 'react'
import Link from 'next/link'
import { ArrowLeftIcon, SparklesIcon, ChartBarIcon } from '@heroicons/react/24/outline'
import DarkModeToggle from '../components/DarkModeToggle'
import AnalyticsDashboard from '../components/AnalyticsDashboard'

export default function AnalyticsPage() {
    return (
        <div className="min-h-screen bg-gradient-to-br from-gray-50 via-white to-primary-50 dark:from-gray-900 dark:via-gray-900 dark:to-gray-800">
            {/* Header */}
            <header className="sticky top-0 z-40 backdrop-blur-xl bg-white/80 dark:bg-gray-900/80 border-b border-gray-200 dark:border-gray-800">
                <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
                    <div className="flex items-center justify-between">
                        <div className="flex items-center gap-4">
                            <Link
                                href="/explore"
                                className="p-2 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-lg transition-colors"
                            >
                                <ArrowLeftIcon className="h-5 w-5 text-gray-600 dark:text-gray-400" />
                            </Link>
                            <div className="flex items-center gap-3">
                                <div className="p-2 bg-primary-100 dark:bg-primary-900/30 rounded-lg">
                                    <ChartBarIcon className="h-6 w-6 text-primary-600 dark:text-primary-400" />
                                </div>
                                <div>
                                    <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
                                        Analytics Dashboard
                                    </h1>
                                    <p className="text-sm text-gray-600 dark:text-gray-400">
                                        System insights and quality metrics
                                    </p>
                                </div>
                                <span className="px-2 py-1 bg-primary-600 text-white text-xs font-semibold rounded-full">
                                    NEW
                                </span>
                            </div>
                        </div>
                        <DarkModeToggle />
                    </div>
                </div>
            </header>

            {/* Main Content */}
            <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
                <div className="mb-6">
                    <div className="bg-gradient-to-r from-primary-500 to-purple-600 rounded-xl p-6 text-white">
                        <div className="flex items-start gap-4">
                            <SparklesIcon className="w-8 h-8 flex-shrink-0" />
                            <div>
                                <h2 className="text-xl font-bold mb-2">Analysis</h2>
                                <p className="text-primary-100">
                                    Comprehensive insights into dataset collection, quality assessment, and recommendation system performance.
                                    All metrics are computed using ML-based evaluation frameworks.
                                </p>
                            </div>
                        </div>
                    </div>
                </div>

                <AnalyticsDashboard />
            </main>
        </div>
    )
}
