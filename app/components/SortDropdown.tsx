'use client'

import { useState, useRef, useEffect } from 'react'
import { ChevronDownIcon, ArrowUpIcon, ArrowDownIcon, FireIcon, ArrowDownTrayIcon, HeartIcon } from '@heroicons/react/24/outline'

export type SortField = 'relevance' | 'downloads' | 'likes' | 'trending'
export type SortOrder = 'asc' | 'desc'

interface SortOption {
    field: SortField
    label: string
    icon: React.ReactNode
}

interface SortDropdownProps {
    sortBy: SortField
    sortOrder: SortOrder
    onSortChange: (field: SortField, order: SortOrder) => void
    showRelevance?: boolean // Only show in AI Finder
}

const SORT_OPTIONS: SortOption[] = [
    { field: 'trending', label: 'Trending', icon: <FireIcon className="h-4 w-4" /> },
    { field: 'downloads', label: 'Downloads', icon: <ArrowDownTrayIcon className="h-4 w-4" /> },
    { field: 'likes', label: 'Likes', icon: <HeartIcon className="h-4 w-4" /> },
]

export default function SortDropdown({ sortBy, sortOrder, onSortChange, showRelevance = false }: SortDropdownProps) {
    const [isOpen, setIsOpen] = useState(false)
    const dropdownRef = useRef<HTMLDivElement>(null)

    // Close dropdown when clicking outside
    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
                setIsOpen(false)
            }
        }
        document.addEventListener('mousedown', handleClickOutside)
        return () => document.removeEventListener('mousedown', handleClickOutside)
    }, [])

    const options = showRelevance
        ? [{ field: 'relevance' as SortField, label: 'Relevance', icon: <FireIcon className="h-4 w-4" /> }, ...SORT_OPTIONS]
        : SORT_OPTIONS

    const currentOption = options.find(o => o.field === sortBy) || options[0]

    const handleFieldChange = (field: SortField) => {
        onSortChange(field, sortOrder)
        setIsOpen(false)
    }

    const toggleOrder = () => {
        onSortChange(sortBy, sortOrder === 'asc' ? 'desc' : 'asc')
    }

    return (
        <div className="flex items-center gap-2">
            {/* Sort By Dropdown */}
            <div className="relative" ref={dropdownRef}>
                <button
                    onClick={() => setIsOpen(!isOpen)}
                    className="flex items-center gap-2 px-4 py-2 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors shadow-sm"
                >
                    {currentOption.icon}
                    <span>{currentOption.label}</span>
                    <ChevronDownIcon className={`h-4 w-4 transition-transform ${isOpen ? 'rotate-180' : ''}`} />
                </button>

                {isOpen && (
                    <div className="absolute top-full left-0 mt-2 w-48 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl shadow-lg z-50 overflow-hidden animate-fade-in">
                        {options.map((option) => (
                            <button
                                key={option.field}
                                onClick={() => handleFieldChange(option.field)}
                                className={`w-full flex items-center gap-3 px-4 py-3 text-sm text-left hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors ${sortBy === option.field
                                        ? 'bg-purple-50 dark:bg-purple-900/30 text-purple-600 dark:text-purple-400'
                                        : 'text-gray-700 dark:text-gray-300'
                                    }`}
                            >
                                {option.icon}
                                <span>{option.label}</span>
                                {sortBy === option.field && (
                                    <span className="ml-auto text-purple-500">✓</span>
                                )}
                            </button>
                        ))}
                    </div>
                )}
            </div>

            {/* Order Toggle */}
            <button
                onClick={toggleOrder}
                className="flex items-center gap-1.5 px-3 py-2 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors shadow-sm"
                title={sortOrder === 'desc' ? 'Descending (highest first)' : 'Ascending (lowest first)'}
            >
                {sortOrder === 'desc' ? (
                    <>
                        <ArrowDownIcon className="h-4 w-4 text-purple-500" />
                        <span className="hidden sm:inline">High to Low</span>
                    </>
                ) : (
                    <>
                        <ArrowUpIcon className="h-4 w-4 text-purple-500" />
                        <span className="hidden sm:inline">Low to High</span>
                    </>
                )}
            </button>
        </div>
    )
}
