'use client'

import { useState, useEffect } from 'react'
import { FunnelIcon, ChartBarIcon } from '@heroicons/react/24/outline'
import DatasetCard from '../components/DatasetCard'
import FilterBar from '../components/FilterBar'
import EmptyState from '../components/EmptyState'
import LoadingCard from '../components/LoadingCard'
import Pagination from '../components/Pagination'
import LayoutHeader from '../components/LayoutHeader'
import SortDropdown, { SortField, SortOrder } from '../components/SortDropdown'
import SmartSearch from '../components/SmartSearch'

interface Dataset {
    id: string
    name: string
    description: string
    domain?: string
    modality?: string
    trend_score?: number
    platform?: string
    gqi_score?: number
}

export default function ExplorePage() {
    const [datasets, setDatasets] = useState<Dataset[]>([])
    const [searchResults, setSearchResults] = useState<Dataset[] | null>(null)
    const [searchIntent, setSearchIntent] = useState<any>(null)
    const [loading, setLoading] = useState(true)
    const [fetchError, setFetchError] = useState<string | null>(null)
    const [searchQuery, setSearchQuery] = useState('')
    const [selectedDomain, setSelectedDomain] = useState<string>('')
    const [selectedModality, setSelectedModality] = useState<string>('')
    const [selectedSource, setSelectedSource] = useState<string>('')
    const [selectedQuality, setSelectedQuality] = useState<string>('')
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
        // Only fetch trending if not in search mode
        if (!searchResults) {
            fetchDatasets()
        }
    }, [selectedDomain, selectedModality, selectedSource, selectedQuality, currentPage, pageSize, searchQuery, sortBy, sortOrder, searchResults])

    const fetchDatasets = async () => {
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
                ...(selectedSource && { platform: selectedSource }),
                ...(selectedQuality && { quality: selectedQuality }),
                ...(searchQuery && { search: searchQuery }),
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

    const handleClearFilters = () => {
        setSelectedDomain('')
        setSelectedModality('')
        setSelectedSource('')
        setSelectedQuality('')
        setSearchQuery('')
        setSearchResults(null)
        setSearchIntent(null)
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

    const domainOptions = [
        { value: '', label: 'All Domains' },
        { value: 'Computer Vision', label: 'Computer Vision' },
        { value: 'NLP', label: 'NLP' },
        { value: 'Audio', label: 'Audio' },
        { value: 'Tabular', label: 'Tabular' },
        { value: 'Multimodal', label: 'Multimodal' },
    ]

    const modalityOptions = [
        { value: '', label: 'All Modalities' },
        { value: 'image', label: 'Image' },
        { value: 'text', label: 'Text' },
        { value: 'audio', label: 'Audio' },
        { value: 'video', label: 'Video' },
        { value: 'tabular', label: 'Tabular' },
    ]

    const sourceOptions = [
        { value: '', label: 'All Sources' },
        { value: 'huggingface', label: 'HuggingFace' },
        { value: 'kaggle', label: 'Kaggle' },
        { value: 'openml', label: 'OpenML' },
        { value: 'zenodo', label: 'Zenodo' },
        { value: 'datagov', label: 'Data.gov' },
        { value: 'aws_opendata', label: 'AWS Open Data' },
        { value: 'harvard_dataverse', label: 'Harvard Dataverse' },
    ]

    const qualityOptions = [
        { value: '', label: 'All Quality Levels' },
        { value: 'excellent', label: 'Excellent (80%+)' },
        { value: 'good', label: 'Good (60-79%)' },
        { value: 'fair', label: 'Fair (40-59%)' },
        { value: 'poor', label: 'Poor (<40%)' },
    ]

    const displayDatasets = searchResults 
        ? searchResults.slice((currentPage - 1) * pageSize, currentPage * pageSize)
        : datasets
    const displayTotal = searchResults ? searchResults.length : totalDatasets

    return (
        <div className="min-h-screen bg-gradient-to-br from-slate-50 via-blue-50 to-slate-100 dark:from-slate-900 dark:via-slate-800 dark:to-slate-900">
            {/* Header */}
            <LayoutHeader />

            {/* Page Header */}
            <section className="bg-gradient-to-br from-primary-600 via-primary-700 to-secondary-600 text-white py-16">
                <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
                    <div className="text-center">
                        <div className="inline-flex items-center gap-2 px-4 py-2 bg-white/10 backdrop-blur-sm rounded-full border border-white/20 mb-4">
                            <FunnelIcon className="h-5 w-5" />
                            <span className="text-sm font-semibold">Explore All Datasets</span>
                        </div>
                        <h1 className="text-4xl md:text-5xl font-bold mb-4">
                            Discover Your Perfect Dataset
                        </h1>
                        <p className="text-xl text-primary-100 max-w-2xl mx-auto">
                            Browse through {totalDatasets > 0 ? totalDatasets.toLocaleString() : '1000+'} datasets with AI-powered search
                        </p>
                    </div>
                </div>
            </section>

            {/* Smart Search Bar */}
            <section className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 -mt-8 mb-8">
                <SmartSearch
                    onSearch={(q) => {
                        setSearchQuery(q)
                        setCurrentPage(1)
                    }}
                    onResults={(results, intent, total) => {
                        setSearchResults(results.length > 0 || intent ? results : null)
                        setSearchIntent(intent)
                        if (results.length > 0) {
                            setTotalDatasets(total)
                            setTotalPages(Math.ceil(total / pageSize))
                        }
                    }}
                    filters={{
                        domain: selectedDomain,
                        modality: selectedModality,
                        platform: selectedSource
                    }}
                    setLoading={setLoading}
                    className="shadow-2xl"
                />
            </section>

            {/* Analytics Link */}
            <section className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
                <a
                    href="/analytics"
                    className="flex items-center gap-3 p-4 bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 hover:border-primary-400 dark:hover:border-primary-600 hover:shadow-lg transition-all group"
                >
                    <div className="p-2 bg-primary-100 dark:bg-primary-900/30 rounded-lg">
                        <ChartBarIcon className="w-6 h-6 text-primary-600 dark:text-primary-400" />
                    </div>
                    <div className="flex-1">
                        <div className="flex items-center gap-2">
                            <h3 className="font-semibold text-gray-900 dark:text-white group-hover:text-primary-600 dark:group-hover:text-primary-400 transition-colors">
                                View Analytics Dashboard
                            </h3>
                            <span className="px-2 py-0.5 bg-primary-600 text-white text-xs font-semibold rounded-full">
                                NEW
                            </span>
                        </div>
                        <p className="text-sm text-gray-600 dark:text-gray-400">
                            System insights, quality metrics, and recommendation performance
                        </p>
                    </div>
                    <span className="text-primary-600 dark:text-primary-400">→</span>
                </a>
            </section>

            {/* Filters & Sorting */}
            <section className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
                <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4 mb-6">
                    <FilterBar
                        domainOptions={domainOptions}
                        modalityOptions={modalityOptions}
                        selectedDomain={selectedDomain}
                        selectedModality={selectedModality}
                        onDomainChange={setSelectedDomain}
                        onModalityChange={setSelectedModality}
                        sourceOptions={sourceOptions}
                        selectedSource={selectedSource}
                        onSourceChange={setSelectedSource}
                        qualityOptions={qualityOptions}
                        selectedQuality={selectedQuality}
                        onQualityChange={setSelectedQuality}
                        onClearFilters={handleClearFilters}
                        datasetCount={displayTotal}
                    />
                    {!searchResults && (
                        <SortDropdown
                            sortBy={sortBy}
                            sortOrder={sortOrder}
                            onSortChange={handleSortChange}
                        />
                    )}
                </div>
            </section>

            {/* Dataset Grid */}
            <section className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pb-16">
                {loading && !searchResults ? (
                    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 sm:gap-6">
                        <LoadingCard count={pageSize} />
                    </div>
                ) : fetchError ? (
                    <EmptyState
                        icon="⚠️"
                        title="Could not load datasets"
                        description={fetchError}
                        action={{
                            label: 'Retry',
                            onClick: fetchDatasets
                        }}
                    />
                ) : displayDatasets.length === 0 ? (
                    <EmptyState
                        icon="🔍"
                        title="No datasets found"
                        description={searchQuery ? `We couldn't find anything for "${searchQuery}"` : "Try adjusting your filters"}
                        action={{
                            label: 'Clear Search',
                            onClick: handleClearFilters
                        }}
                    />
                ) : (
                    <>
                        {searchResults && (
                            <div className="mb-6 flex items-center justify-between">
                                <h3 className="text-lg font-bold text-gray-900 dark:text-white">
                                    Search Results ({totalDatasets})
                                </h3>
                                <button 
                                    onClick={handleClearFilters}
                                    className="text-sm text-primary-600 hover:text-primary-700 font-medium"
                                >
                                    Clear Search
                                </button>
                            </div>
                        )}
                        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 sm:gap-6">
                            {displayDatasets.map((dataset) => (
                                <DatasetCard
                                    key={dataset.id}
                                    dataset={dataset}
                                    onClick={handleDatasetClick}
                                />
                            ))}
                        </div>

                        {/* Pagination - Show for both search and trending */}
                        {((!searchResults && datasets.length > 0) || (searchResults && searchResults.length > pageSize)) && (
                            <Pagination
                                currentPage={currentPage}
                                totalPages={searchResults ? Math.ceil(searchResults.length / pageSize) : totalPages}
                                totalItems={searchResults ? searchResults.length : totalDatasets}
                                itemsPerPage={pageSize}
                                onPageChange={handlePageChange}
                                onPageSizeChange={handlePageSizeChange}
                            />
                        )}
                    </>
                )}
            </section>
        </div>
    )
}

