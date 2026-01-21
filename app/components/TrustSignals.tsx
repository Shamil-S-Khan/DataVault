'use client'

import { EyeIcon, ArrowDownTrayIcon, HeartIcon, ClockIcon } from '@heroicons/react/24/outline'

interface TrustSignalsProps {
    views?: number
    downloads?: number
    likes?: number
    lastUpdated?: string
    verified?: boolean
}

export default function TrustSignals({ views, downloads, likes, lastUpdated, verified }: TrustSignalsProps) {
    const signals = [
        { icon: EyeIcon, value: views, label: 'views', color: 'text-blue-600 dark:text-blue-400' },
        { icon: ArrowDownTrayIcon, value: downloads, label: 'downloads', color: 'text-green-600 dark:text-green-400' },
        { icon: HeartIcon, value: likes, label: 'likes', color: 'text-red-600 dark:text-red-400' },
    ].filter(signal => signal.value !== undefined && signal.value > 0)

    if (signals.length === 0 && !lastUpdated && !verified) return null

    const formatNumber = (num: number) => {
        if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`
        if (num >= 1000) return `${(num / 1000).toFixed(1)}K`
        return num.toLocaleString()
    }

    return (
        <div className="flex flex-wrap items-center gap-4 text-sm">
            {signals.map((signal, index) => {
                const Icon = signal.icon
                return (
                    <div key={index} className="flex items-center gap-1.5">
                        <Icon className={`h-4 w-4 ${signal.color}`} />
                        <span className="font-semibold text-gray-900 dark:text-white">
                            {formatNumber(signal.value!)}
                        </span>
                        <span className="text-gray-500 dark:text-gray-400">
                            {signal.label}
                        </span>
                    </div>
                )
            })}

            {lastUpdated && (
                <div className="flex items-center gap-1.5">
                    <ClockIcon className="h-4 w-4 text-gray-500 dark:text-gray-400" />
                    <span className="text-gray-600 dark:text-gray-400">
                        Updated {new Date(lastUpdated).toLocaleDateString()}
                    </span>
                </div>
            )}

            {verified && (
                <div className="flex items-center gap-1.5 px-2 py-1 bg-primary-50 dark:bg-primary-900/20 rounded-full border border-primary-200 dark:border-primary-800">
                    <svg className="h-4 w-4 text-primary-600 dark:text-primary-400" fill="currentColor" viewBox="0 0 20 20">
                        <path fillRule="evenodd" d="M6.267 3.455a3.066 3.066 0 001.745-.723 3.066 3.066 0 013.976 0 3.066 3.066 0 001.745.723 3.066 3.066 0 012.812 2.812c.051.643.304 1.254.723 1.745a3.066 3.066 0 010 3.976 3.066 3.066 0 00-.723 1.745 3.066 3.066 0 01-2.812 2.812 3.066 3.066 0 00-1.745.723 3.066 3.066 0 01-3.976 0 3.066 3.066 0 00-1.745-.723 3.066 3.066 0 01-2.812-2.812 3.066 3.066 0 00-.723-1.745 3.066 3.066 0 010-3.976 3.066 3.066 0 00.723-1.745 3.066 3.066 0 012.812-2.812zm7.44 5.252a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                    </svg>
                    <span className="text-xs font-semibold text-primary-700 dark:text-primary-300">
                        Verified
                    </span>
                </div>
            )}
        </div>
    )
}
