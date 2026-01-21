'use client'

import {
    HashtagIcon,
    DocumentTextIcon,
    CalendarIcon,
    CheckCircleIcon,
    ListBulletIcon,
    PhotoIcon,
    MusicalNoteIcon,
    VideoCameraIcon
} from '@heroicons/react/24/outline'

interface DataTypeBadgeProps {
    type: string
    size?: 'sm' | 'md'
}

const typeConfig: Record<string, { color: string; icon: any; label: string }> = {
    // Numeric types
    'int': { color: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300 border-blue-200 dark:border-blue-800', icon: HashtagIcon, label: 'Integer' },
    'int8': { color: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300 border-blue-200 dark:border-blue-800', icon: HashtagIcon, label: 'Int8' },
    'int16': { color: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300 border-blue-200 dark:border-blue-800', icon: HashtagIcon, label: 'Int16' },
    'int32': { color: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300 border-blue-200 dark:border-blue-800', icon: HashtagIcon, label: 'Int32' },
    'int64': { color: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300 border-blue-200 dark:border-blue-800', icon: HashtagIcon, label: 'Int64' },
    'float': { color: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300 border-blue-200 dark:border-blue-800', icon: HashtagIcon, label: 'Float' },
    'float32': { color: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300 border-blue-200 dark:border-blue-800', icon: HashtagIcon, label: 'Float32' },
    'float64': { color: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300 border-blue-200 dark:border-blue-800', icon: HashtagIcon, label: 'Float64' },
    'number': { color: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300 border-blue-200 dark:border-blue-800', icon: HashtagIcon, label: 'Number' },

    // Text types
    'string': { color: 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300 border-green-200 dark:border-green-800', icon: DocumentTextIcon, label: 'Text' },
    'text': { color: 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300 border-green-200 dark:border-green-800', icon: DocumentTextIcon, label: 'Text' },

    // Categorical
    'category': { color: 'bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-300 border-purple-200 dark:border-purple-800', icon: ListBulletIcon, label: 'Category' },
    'categorical': { color: 'bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-300 border-purple-200 dark:border-purple-800', icon: ListBulletIcon, label: 'Category' },

    // Time-series
    'datetime': { color: 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300 border-amber-200 dark:border-amber-800', icon: CalendarIcon, label: 'DateTime' },
    'timestamp': { color: 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300 border-amber-200 dark:border-amber-800', icon: CalendarIcon, label: 'Timestamp' },
    'date': { color: 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300 border-amber-200 dark:border-amber-800', icon: CalendarIcon, label: 'Date' },

    // Boolean
    'bool': { color: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300 border-red-200 dark:border-red-800', icon: CheckCircleIcon, label: 'Boolean' },
    'boolean': { color: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300 border-red-200 dark:border-red-800', icon: CheckCircleIcon, label: 'Boolean' },

    // Media types
    'image': { color: 'bg-pink-100 text-pink-700 dark:bg-pink-900/30 dark:text-pink-300 border-pink-200 dark:border-pink-800', icon: PhotoIcon, label: 'Image' },
    'audio': { color: 'bg-indigo-100 text-indigo-700 dark:bg-indigo-900/30 dark:text-indigo-300 border-indigo-200 dark:border-indigo-800', icon: MusicalNoteIcon, label: 'Audio' },
    'video': { color: 'bg-violet-100 text-violet-700 dark:bg-violet-900/30 dark:text-violet-300 border-violet-200 dark:border-violet-800', icon: VideoCameraIcon, label: 'Video' },
}

export default function DataTypeBadge({ type, size = 'md' }: DataTypeBadgeProps) {
    const normalizedType = type?.toLowerCase() || 'string'
    const config = typeConfig[normalizedType] || typeConfig['string']
    const Icon = config.icon

    const sizeClasses = size === 'sm'
        ? 'px-2 py-0.5 text-[10px]'
        : 'px-2.5 py-1 text-xs'

    return (
        <span className={`inline-flex items-center gap-1 rounded-md font-bold uppercase tracking-wider border ${config.color} ${sizeClasses} transition-all hover:scale-105`}>
            <Icon className={size === 'sm' ? 'h-3 w-3' : 'h-3.5 w-3.5'} />
            {config.label}
        </span>
    )
}
