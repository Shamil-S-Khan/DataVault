'use client'

import { useState, useEffect } from 'react'
import dynamic from 'next/dynamic'
import { MagnifyingGlassIcon, FireIcon, SparklesIcon } from '@heroicons/react/24/outline'
import DatasetCard from './components/DatasetCard'
import FilterBar from './components/FilterBar'
import EmptyState from './components/EmptyState'
import LoadingCard from './components/LoadingCard'
import LayoutHeader from './components/LayoutHeader'
import Pagination from './components/Pagination'
import SortDropdown, { SortField, SortOrder } from './components/SortDropdown'

// Lazy load SemanticSearch for better performance
const SemanticSearch = dynamic(() => import('./components/SemanticSearch'), {
    loading: () => <div className="animate-pulse h-12 bg-gray-200 dark:bg-gray-700 rounded-lg" />,
    ssr: false
})

interface Dataset {
    id: string
    name: string
    description: string
    domain?: string
    modality?: string
    trend_score?: number
    quality_score?: number
}

const domainOptions = [
    { value: '', label: 'All Domains' },
    { value: 'Computer Vision', label: 'Computer Vision' },
    { value: 'Natural Language Processing', label: 'NLP' },
    { value: 'Audio', label: 'Audio' },
    { value: 'Tabular', label: 'Tabular' },
]

const modalityOptions = [
    { value: '', label: 'All Modalities' },
    { value: 'image', label: 'Image' },
    { value: 'text', label: 'Text' },
    { value: 'audio', label: 'Audio' },
    { value: 'video', label: 'Video' },
    { value: 'tabular', label: 'Tabular' },
]

export default function HomePage() {
    const [datasets, setDatasets] = useState<Dataset[]>([])
    const [loading, setLoading] = useState(true)
    const [fetchError, setFetchError] = useState<string | null>(null)
    const [searchQuery, setSearchQuery] = useState('')
    const [selectedDomain, setSelectedDomain] = useState<string>('')
    const [selectedModality, setSelectedModality] = useState<string>('')
    const [currentPage, setCurrentPage] = useState(1)
    const [pageSize, setPageSize] = useState(24)
    const [totalDatasets, setTotalDatasets] = useState(0)
    const [totalPages, setTotalPages] = useState(0)
    const [sortBy, setSortBy] = useState<SortField>('trending')
    const [sortOrder, setSortOrder] = useState<SortOrder>('desc')

    const handleDatasetClick = (id: string) => {
        window.location.href = `/dataset/${id}`
    }

    useEffect(() => {
        fetchTrendingDatasets()
    }, [selectedDomain, selectedModality, currentPage, pageSize, sortBy, sortOrder])

    const fetchTrendingDatasets = async () => {
        setLoading(true)
        setFetchError(null)
        try {
            const apiUrl = (process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001').replace(/\/api\/?$/, '')
            const params = new URLSearchParams({
                page: currentPage.toString(),
                limit: pageSize.toString(),
                sort_by: sortBy,
                sort_order: sortOrder,
                ...(selectedDomain && { domain: selectedDomain }),
                ...(selectedModality && { modality: selectedModality }),
            })

            const response = await fetch(`${apiUrl}/api/datasets/trending?${params}`)
            if (!response.ok) {
                throw new Error(`Failed to fetch datasets (${response.status})`)
            }
            const data = await response.json()

            setDatasets(data.datasets || [])
            setTotalDatasets(data.pagination?.total || data.total || 0)
            setTotalPages(data.pagination?.pages || data.pages || Math.ceil((data.total || 0) / pageSize))
        } catch (error) {
            console.error('Failed to fetch datasets:', error)
            setFetchError('Unable to load datasets right now. Please try again in a moment.')
        } finally {
            setLoading(false)
        }
    }

    const handleSearch = async (e: React.FormEvent) => {
        e.preventDefault()
        if (!searchQuery.trim()) return

        setLoading(true)
        setFetchError(null)
        try {
            const apiUrl = (process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001').replace(/\/api\/?$/, '')
            const response = await fetch(`${apiUrl}/api/datasets/search?query=${encodeURIComponent(searchQuery)}`, {
                method: 'POST',
            })
            if (!response.ok) {
                throw new Error(`Search failed (${response.status})`)
            }
            const data = await response.json()

            setDatasets(data.datasets || [])
        } catch (error) {
            console.error('Search failed:', error)
            setFetchError('Search is temporarily unavailable. Please retry.')
        } finally {
            setLoading(false)
        }
    }

    const handleClearFilters = () => {
        setSelectedDomain('')
        setSelectedModality('')
        setSortBy('trending')
        setSortOrder('desc')
        setCurrentPage(1)
    }

    const handleSortChange = (field: SortField, order: SortOrder) => {
        setSortBy(field)
        setSortOrder(order)
        setCurrentPage(1)
    }

    const handlePageChange = (page: number) => {
        setCurrentPage(page)
        window.scrollTo({ top: 0, behavior: 'smooth' })
    }

    const handlePageSizeChange = (size: number) => {
        setPageSize(size)
        setCurrentPage(1)
    }

    return (
        <div className="min-h-screen">
            <LayoutHeader />
            
            {/* main content wrapper */}
            <div>
                {/* Hero Section */}
                <section className="relative overflow-hidden bg-gradient-to-br from-primary-600 via-primary-700 to-secondary-600 text-white py-20">
                    {/* Decorative Elements */}
                    <div className="absolute top-0 left-0 w-96 h-96 bg-primary-400/30 rounded-full blur-3xl -translate-x-1/2 -translate-y-1/2"></div>
                    <div className="absolute bottom-0 right-0 w-96 h-96 bg-secondary-400/30 rounded-full blur-3xl translate-x-1/2 translate-y-1/2"></div>

                    <div className="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
                        <div className="text-center">
                        <h2 className="text-4xl md:text-5xl font-bold mb-4 animate-fade-in">
                            Track the Future of AI Datasets
                        </h2>
                        <p className="text-xl mb-10 text-primary-100 max-w-2xl mx-auto animate-fade-in">
                            Discover trending datasets, analyze growth patterns, and predict the next big thing
                        </p>

                        {/* Enhanced Search Bar */}
                        <form onSubmit={handleSearch} className="max-w-3xl mx-auto animate-scale-in">
                            <div className="flex gap-3">
                                <div className="flex-1 relative group">
                                    <MagnifyingGlassIcon className="absolute left-4 top-1/2 transform -translate-y-1/2 h-6 w-6 text-gray-400 group-focus-within:text-primary-500 transition-colors" />
                                    <input
                                        type="text"
                                        value={searchQuery}
                                        onChange={(e) => setSearchQuery(e.target.value)}
                                        placeholder="Search for datasets by name, description, or tags..."
                                        className="w-full pl-12 pr-4 py-4 rounded-xl text-gray-900 dark:text-white bg-white dark:bg-gray-800 focus:outline-none focus:ring-4 focus:ring-primary-300 dark:focus:ring-primary-700 shadow-xl transition-all duration-200 placeholder:text-gray-400"
                                    />
                                </div>
                                <button
                                    type="submit"
                                    className="px-8 py-4 bg-white text-primary-600 rounded-xl font-bold hover:bg-primary-50 transition-all duration-200 shadow-xl hover:shadow-2xl hover:scale-105"
                                >
                                    Search
                                </button>
                            </div>
                        </form>

                        {/* CTA Buttons */}
                        <div className="flex flex-col sm:flex-row items-center justify-center gap-4 mt-8">
                            <button
                                onClick={() => window.location.href = '/explore'}
                                className="px-8 py-3 bg-primary-600 hover:bg-primary-700 text-white rounded-xl font-semibold shadow-lg hover:shadow-xl transition-all duration-200 hover:scale-105"
                            >
                                Explore Datasets
                            </button>
                            <button className="px-8 py-3 bg-transparent border-2 border-white hover:bg-white/10 text-white rounded-xl font-semibold transition-all duration-200 hover:scale-105">
                                Sign Up
                            </button>
                        </div>
                    </div>
                </div>
            </section>

            {/* Filters */}
            <section className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 -mt-8 relative z-10">
                <FilterBar
                    domainOptions={domainOptions}
                    modalityOptions={modalityOptions}
                    selectedDomain={selectedDomain}
                    selectedModality={selectedModality}
                    onDomainChange={setSelectedDomain}
                    onModalityChange={setSelectedModality}
                    onClearFilters={handleClearFilters}
                    datasetCount={totalDatasets}
                />
            </section>

            {/* Trending Datasets */}
            <section className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 pb-16">
                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-8">
                    <div className="flex items-center gap-3">
                        <div className="p-2 rounded-xl bg-gradient-to-br from-orange-500 to-red-500 shadow-lg">
                            <FireIcon className="h-6 w-6 text-white" />
                        </div>
                        <div>
                            <h3 className="text-3xl font-bold text-gray-900 dark:text-white">
                                Trending Datasets
                            </h3>
                            <p className="text-sm text-gray-600 dark:text-gray-400">
                                {totalDatasets > 0 ? `${totalDatasets.toLocaleString()} datasets available` : 'Loading...'}
                            </p>
                        </div>
                    </div>
                    <SortDropdown
                        sortBy={sortBy}
                        sortOrder={sortOrder}
                        onSortChange={handleSortChange}
                    />
                </div>

                {loading ? (
                    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 sm:gap-6">
                        <LoadingCard count={6} />
                    </div>
                ) : fetchError ? (
                    <EmptyState
                        icon="⚠️"
                        title="Could not load datasets"
                        description={fetchError}
                        action={{
                            label: 'Retry',
                            onClick: fetchTrendingDatasets
                        }}
                    />
                ) : datasets.length === 0 && !loading ? (
                    <EmptyState
                        icon="🔍"
                        title="No datasets found"
                        description="Try adjusting your filters or search query to discover more datasets."
                        action={{
                            label: 'Clear Filters',
                            onClick: handleClearFilters
                        }}
                    />
                ) : datasets.length > 0 ? (
                    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 sm:gap-6">
                        {datasets.map((dataset) => (
                            <DatasetCard
                                key={dataset.id}
                                dataset={dataset}
                                onClick={handleDatasetClick}
                            />
                        ))}
                    </div>
                ) : (
                    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 sm:gap-6">
                        <DatasetCard
                            dataset={{
                                id: 'coco-2017',
                                name: 'COCO-2017',
                                description: 'A large-scale object detection, segmentation, and captioning dataset.',
                                domain: 'Computer Vision',
                                modality: 'image',
                                trend_score: 0.92
                            }}
                            onClick={handleDatasetClick}
                        />
                        <DatasetCard
                            dataset={{
                                id: 'the-pile',
                                name: 'The Pile',
                                description: 'A diverse, 825 GiB text corpus for large language model training.',
                                domain: 'NLP',
                                modality: 'text',
                                trend_score: 0.88
                            }}
                            onClick={handleDatasetClick}
                        />
                        <DatasetCard
                            dataset={{
                                id: 'laion-5b',
                                name: 'LAION-5B',
                                description: 'An open, large-scale dataset of 5.85 billion CLIP-filtered image-text pairs.',
                                domain: 'Image-Text',
                                modality: 'image',
                                trend_score: 0.95
                            }}
                            onClick={handleDatasetClick}
                        />
                    </div>
                )}

                {/* Pagination */}
                {!loading && datasets.length > 0 && totalPages > 1 && (
                    <Pagination
                        currentPage={currentPage}
                        totalPages={totalPages}
                        totalItems={totalDatasets}
                        itemsPerPage={pageSize}
                        onPageChange={handlePageChange}
                        onPageSizeChange={handlePageSizeChange}
                    />
                )}
            </section>

            {/* Footer / Trusted By Section */}
            <footer className="bg-white dark:bg-gray-900 border-t border-gray-200 dark:border-gray-800">
                <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-16">
                    <h3 className="text-2xl md:text-3xl font-bold text-center text-gray-900 dark:text-white mb-12">
                        Trusted by Leading AI Teams
                    </h3>
                    <div className="flex flex-wrap items-center justify-center gap-8 md:gap-12">
                        <div className="px-6 py-4 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
                            <span className="text-lg font-semibold text-gray-700 dark:text-gray-300">[University]</span>
                        </div>
                        <div className="px-6 py-4 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
                            <span className="text-lg font-semibold text-gray-700 dark:text-gray-300">[AI Startup]</span>
                        </div>
                        <div className="px-6 py-4 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
                            <span className="text-lg font-semibold text-gray-700 dark:text-gray-300">[Big Tech]</span>
                        </div>
                        <div className="px-6 py-4 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
                            <span className="text-lg font-semibold text-gray-700 dark:text-gray-300">[Research Lab]</span>
                        </div>
                        <div className="px-6 py-4 bg-gray-100 dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
                            <span className="text-lg font-semibold text-gray-700 dark:text-gray-300">[ML Platform]</span>
                        </div>
                    </div>
                </div>
            </footer>
            </div>
        </div>
    )
}
