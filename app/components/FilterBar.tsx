'use client'

import { XMarkIcon } from '@heroicons/react/24/outline'

interface FilterOption {
    value: string
    label: string
}

interface FilterBarProps {
    domainOptions: FilterOption[]
    modalityOptions: FilterOption[]
    sourceOptions?: FilterOption[]
    qualityOptions?: FilterOption[]
    selectedDomain: string
    selectedModality: string
    selectedSource?: string
    selectedQuality?: string
    onDomainChange: (value: string) => void
    onModalityChange: (value: string) => void
    onSourceChange?: (value: string) => void
    onQualityChange?: (value: string) => void
    onClearFilters: () => void
    datasetCount?: number
}

export default function FilterBar({
    domainOptions,
    modalityOptions,
    sourceOptions,
    qualityOptions,
    selectedDomain,
    selectedModality,
    selectedSource,
    selectedQuality,
    onDomainChange,
    onModalityChange,
    onSourceChange,
    onQualityChange,
    onClearFilters,
    datasetCount
}: FilterBarProps) {
    const hasActiveFilters = selectedDomain || selectedModality || selectedSource || selectedQuality

    return (
        <div className="rounded-xl sm:rounded-2xl p-4 sm:p-6 bg-white/80 dark:bg-gray-900/80 backdrop-blur-md border border-gray-200/60 dark:border-gray-700/50 shadow-sm dark:shadow-glass">
            {/* Dataset Count */}
            {datasetCount !== undefined && (
                <div className="mb-3 sm:mb-4 pb-3 sm:pb-4 border-b border-gray-200 dark:border-gray-700">
                    <p className="text-xs sm:text-sm text-gray-600 dark:text-gray-400">
                        <span className="font-bold text-xl sm:text-2xl text-primary-600 dark:text-primary-400">
                            {datasetCount.toLocaleString()}
                        </span>
                        {' '}datasets match your filters
                    </p>
                </div>
            )}

            {/* Filter Controls */}
            <div className="flex flex-col gap-3 sm:gap-4">
                {/* Filter Controls */}
                <div className="grid grid-cols-2 sm:flex sm:flex-wrap gap-2 sm:gap-3 flex-1">
                    {/* Domain Filter */}
                    <div className="relative">
                        <select
                            value={selectedDomain}
                            onChange={(e) => onDomainChange(e.target.value)}
                            className="appearance-none w-full px-3 sm:px-4 py-2 sm:py-2.5 pr-8 sm:pr-10 bg-white dark:bg-gray-800 border-2 border-gray-200 dark:border-gray-700 rounded-lg sm:rounded-xl text-xs sm:text-sm text-gray-900 dark:text-white font-medium focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all duration-200 hover:border-primary-300 dark:hover:border-primary-600 cursor-pointer"
                        >
                            {domainOptions.map(option => (
                                <option key={option.value} value={option.value}>
                                    {option.label}
                                </option>
                            ))}
                        </select>
                        <div className="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none">
                            <svg className="w-5 h-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                            </svg>
                        </div>
                    </div>

                    {/* Modality Filter */}
                    <div className="relative">
                        <select
                            value={selectedModality}
                            onChange={(e) => onModalityChange(e.target.value)}
                            className="appearance-none w-full px-3 sm:px-4 py-2 sm:py-2.5 pr-8 sm:pr-10 bg-white dark:bg-gray-800 border-2 border-gray-200 dark:border-gray-700 rounded-lg sm:rounded-xl text-xs sm:text-sm text-gray-900 dark:text-white font-medium focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all duration-200 hover:border-primary-300 dark:hover:border-primary-600 cursor-pointer"
                        >
                            {modalityOptions.map(option => (
                                <option key={option.value} value={option.value}>
                                    {option.label}
                                </option>
                            ))}
                        </select>
                        <div className="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none">
                            <svg className="w-5 h-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                            </svg>
                        </div>
                    </div>

                    {/* Source Filter */}
                    {sourceOptions && onSourceChange && (
                        <div className="relative">
                            <select
                                value={selectedSource || ''}
                                onChange={(e) => onSourceChange(e.target.value)}
                                className="appearance-none w-full px-3 sm:px-4 py-2 sm:py-2.5 pr-8 sm:pr-10 bg-white dark:bg-gray-800 border-2 border-gray-200 dark:border-gray-700 rounded-lg sm:rounded-xl text-xs sm:text-sm text-gray-900 dark:text-white font-medium focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all duration-200 hover:border-primary-300 dark:hover:border-primary-600 cursor-pointer"
                            >
                                {sourceOptions.map(option => (
                                    <option key={option.value} value={option.value}>
                                        {option.label}
                                    </option>
                                ))}
                            </select>
                            <div className="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none">
                                <svg className="w-5 h-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                                </svg>
                            </div>
                        </div>
                    )}

                    {/* Quality Filter */}
                    {qualityOptions && onQualityChange && (
                        <div className="relative">
                            <select
                                value={selectedQuality || ''}
                                onChange={(e) => onQualityChange(e.target.value)}
                                className="appearance-none w-full px-3 sm:px-4 py-2 sm:py-2.5 pr-8 sm:pr-10 bg-white dark:bg-gray-800 border-2 border-gray-200 dark:border-gray-700 rounded-lg sm:rounded-xl text-xs sm:text-sm text-gray-900 dark:text-white font-medium focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent transition-all duration-200 hover:border-primary-300 dark:hover:border-primary-600 cursor-pointer"
                            >
                                {qualityOptions.map(option => (
                                    <option key={option.value} value={option.value}>
                                        {option.label}
                                    </option>
                                ))}
                            </select>
                            <div className="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none">
                                <svg className="w-5 h-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                                </svg>
                            </div>
                        </div>
                    )}
                </div>

                {/* Clear Filters Button */}
                {hasActiveFilters && (
                    <button
                        onClick={onClearFilters}
                        className="flex items-center justify-center gap-2 px-3 sm:px-4 py-2 sm:py-2.5 bg-gray-100 dark:bg-gray-800 text-gray-700 dark:text-gray-300 rounded-lg sm:rounded-xl hover:bg-gray-200 dark:hover:bg-gray-700 transition-all duration-200 font-medium text-xs sm:text-sm border-2 border-transparent hover:border-gray-300 dark:hover:border-gray-600 w-full sm:w-auto"
                    >
                        <XMarkIcon className="h-5 w-5" />
                        <span>Clear Filters</span>
                    </button>
                )}
            </div>

            {/* Active Filters Display */}
            {hasActiveFilters && (
                <div className="mt-4 pt-4 border-t border-gray-200 dark:border-gray-700">
                    <div className="flex flex-wrap gap-2 items-center">
                        <span className="text-sm font-medium text-gray-600 dark:text-gray-400">
                            Active filters:
                        </span>
                        {selectedDomain && (
                            <span className="inline-flex items-center gap-1.5 px-3 py-1 bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 rounded-full text-sm font-medium">
                                {domainOptions.find(opt => opt.value === selectedDomain)?.label}
                                <button
                                    onClick={() => onDomainChange('')}
                                    className="hover:bg-blue-200 dark:hover:bg-blue-800 rounded-full p-0.5 transition-colors"
                                >
                                    <XMarkIcon className="h-3.5 w-3.5" />
                                </button>
                            </span>
                        )}
                        {selectedModality && (
                            <span className="inline-flex items-center gap-1.5 px-3 py-1 bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300 rounded-full text-sm font-medium">
                                {modalityOptions.find(opt => opt.value === selectedModality)?.label}
                                <button
                                    onClick={() => onModalityChange('')}
                                    className="hover:bg-purple-200 dark:hover:bg-purple-800 rounded-full p-0.5 transition-colors"
                                >
                                    <XMarkIcon className="h-3.5 w-3.5" />
                                </button>
                            </span>
                        )}
                        {selectedSource && sourceOptions && onSourceChange && (
                            <span className="inline-flex items-center gap-1.5 px-3 py-1 bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300 rounded-full text-sm font-medium">
                                {sourceOptions.find(opt => opt.value === selectedSource)?.label}
                                <button
                                    onClick={() => onSourceChange('')}
                                    className="hover:bg-green-200 dark:hover:bg-green-800 rounded-full p-0.5 transition-colors"
                                >
                                    <XMarkIcon className="h-3.5 w-3.5" />
                                </button>
                            </span>
                        )}
                        {selectedQuality && qualityOptions && onQualityChange && (
                            <span className="inline-flex items-center gap-1.5 px-3 py-1 bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-300 rounded-full text-sm font-medium">
                                {qualityOptions.find(opt => opt.value === selectedQuality)?.label}
                                <button
                                    onClick={() => onQualityChange('')}
                                    className="hover:bg-orange-200 dark:hover:bg-orange-800 rounded-full p-0.5 transition-colors"
                                >
                                    <XMarkIcon className="h-3.5 w-3.5" />
                                </button>
                            </span>
                        )}
                    </div>
                </div>
            )}
        </div>
    )
}
