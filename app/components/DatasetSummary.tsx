'use client'

import { GlobeAltIcon, TagIcon, CalendarIcon, ScaleIcon, DocumentTextIcon, KeyIcon } from '@heroicons/react/24/outline'

interface DatasetSummaryProps {
    source?: string
    domain?: string
    modality?: string
    rows?: number
    license?: string
    lastUpdated?: string
    taskType?: string
}

export default function DatasetSummary({
    source,
    domain,
    modality,
    rows,
    license,
    lastUpdated,
    taskType
}: DatasetSummaryProps) {
    const summaryItems = [
        { icon: GlobeAltIcon, label: 'Source', value: source },
        { icon: TagIcon, label: 'Domain', value: domain },
        { icon: DocumentTextIcon, label: 'Modality', value: modality },
        { icon: ScaleIcon, label: 'Rows', value: rows?.toLocaleString() },
        { icon: KeyIcon, label: 'License', value: license },
        { icon: CalendarIcon, label: 'Last Updated', value: lastUpdated ? new Date(lastUpdated).toLocaleDateString() : undefined },
        { icon: TagIcon, label: 'Task Type', value: taskType },
    ].filter(item => item.value)

    if (summaryItems.length === 0) return null

    return (
        <div className="glass rounded-2xl p-6 shadow-glass border border-gray-200 dark:border-gray-700 mb-8">
            <h3 className="text-lg font-bold text-gray-900 dark:text-white mb-4">Dataset Summary</h3>
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
                {summaryItems.map((item, index) => {
                    const Icon = item.icon
                    return (
                        <div key={index} className="flex items-start gap-3">
                            <div className="p-2 rounded-lg bg-primary-50 dark:bg-primary-900/20">
                                <Icon className="h-5 w-5 text-primary-600 dark:text-primary-400" />
                            </div>
                            <div className="flex-1 min-w-0">
                                <p className="text-xs text-gray-500 dark:text-gray-400 font-medium uppercase tracking-wide">
                                    {item.label}
                                </p>
                                <p className="text-sm font-semibold text-gray-900 dark:text-white truncate mt-0.5">
                                    {item.value}
                                </p>
                            </div>
                        </div>
                    )
                })}
            </div>
        </div>
    )
}
