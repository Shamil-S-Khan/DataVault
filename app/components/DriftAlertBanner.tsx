'use client'

import { ExclamationTriangleIcon, BellIcon, BellSlashIcon } from '@heroicons/react/24/outline'
import { useState } from 'react'

interface DriftAlert {
    field: string
    display: string
    change: number
    severity: string
}

interface DriftAlertBannerProps {
    datasetId: string
    driftScore: number
    alerts: DriftAlert[]
    isStarred: boolean
    onToggleStar: () => void
}

export default function DriftAlertBanner({ 
    datasetId, 
    driftScore, 
    alerts, 
    isStarred,
    onToggleStar 
}: DriftAlertBannerProps) {
    const [isToggling, setIsToggling] = useState(false)

    if (driftScore < 30) {
        // Only show "Follow" button if no major drift
        return (
            <div className="flex items-center justify-between p-4 glass rounded-2xl border border-gray-200 dark:border-gray-700 mb-6 bg-white/40 dark:bg-gray-900/40">
                <div className="flex items-center gap-3">
                    <div className={`p-2 rounded-lg ${isStarred ? 'bg-amber-100 dark:bg-amber-900/30 text-amber-600' : 'bg-gray-100 dark:bg-gray-800 text-gray-500'}`}>
                        <BellIcon className="h-5 w-5" />
                    </div>
                    <div>
                        <h4 className="text-sm font-bold text-gray-900 dark:text-white">
                            {isStarred ? 'Following Dataset' : 'Follow for Updates'}
                        </h4>
                        <p className="text-xs text-gray-500 dark:text-gray-400">
                            {isStarred ? "You'll be notified of significant distribution drifts." : 'Get alerted when major changes occur in new versions.'}
                        </p>
                    </div>
                </div>
                <button
                    onClick={async () => {
                        setIsToggling(true)
                        await onToggleStar()
                        setIsToggling(false)
                    }}
                    disabled={isToggling}
                    className={`px-4 py-2 rounded-xl text-xs font-bold transition-all ${
                        isStarred 
                        ? 'bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400 hover:bg-gray-200' 
                        : 'bg-primary-500 text-white hover:bg-primary-600 shadow-md hover:shadow-lg'
                    }`}
                >
                    {isToggling ? '...' : isStarred ? 'Unfollow' : 'Follow'}
                </button>
            </div>
        )
    }

    const severityColor = driftScore >= 60 ? 'red' : 'orange'

    return (
        <div className={`p-6 rounded-2xl border-2 mb-6 shadow-xl animate-in fade-in slide-in-from-top-4 duration-500 ${
            severityColor === 'red' 
            ? 'bg-red-50 border-red-200 dark:bg-red-900/10 dark:border-red-800/50' 
            : 'bg-orange-50 border-orange-200 dark:bg-orange-900/10 dark:border-orange-800/50'
        }`}>
            <div className="flex flex-col md:flex-row gap-6 items-start">
                <div className={`p-4 rounded-2xl shrink-0 ${
                    severityColor === 'red' ? 'bg-red-500 text-white' : 'bg-orange-500 text-white'
                }`}>
                    <ExclamationTriangleIcon className="h-8 w-8" />
                </div>
                
                <div className="flex-1">
                    <div className="flex flex-wrap items-center justify-between gap-4 mb-4">
                        <div>
                            <h3 className={`text-xl font-black uppercase tracking-tight ${
                                severityColor === 'red' ? 'text-red-700 dark:text-red-400' : 'text-orange-700 dark:text-orange-400'
                            }`}>
                                Dataset Drift Warning: {driftScore >= 60 ? 'CRITICAL' : 'SIGNIFICANT'}
                            </h3>
                            <p className="text-sm text-gray-600 dark:text-gray-400 font-medium">
                                The latest version shows major distribution changes detected during synchronization.
                            </p>
                        </div>
                        
                        <button
                            onClick={async () => {
                                setIsToggling(true)
                                await onToggleStar()
                                setIsToggling(false)
                            }}
                            className={`flex items-center gap-2 px-4 py-2 rounded-xl text-xs font-bold border transition-all ${
                                isStarred 
                                ? 'bg-white dark:bg-gray-900 border-gray-200 dark:border-gray-700 text-gray-500' 
                                : severityColor === 'red' 
                                    ? 'bg-red-500 border-red-500 text-white hover:bg-red-600' 
                                    : 'bg-orange-500 border-orange-500 text-white hover:bg-orange-600'
                            }`}
                        >
                            {isStarred ? <BellSlashIcon className="h-4 w-4" /> : <BellIcon className="h-4 w-4" />}
                            {isStarred ? 'Disable Alerts' : 'Enable Emergency Alerts'}
                        </button>
                    </div>

                    <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                        {alerts.map((alert, idx) => (
                            <div key={idx} className="flex items-center justify-between p-3 bg-white/50 dark:bg-black/20 rounded-xl border border-white/50 dark:border-white/5 shadow-sm">
                                <span className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase">{alert.display}</span>
                                <div className="flex items-center gap-2">
                                    <span className={`text-sm font-black ${alert.change > 0 ? 'text-emerald-500' : 'text-red-500'}`}>
                                        {alert.change > 0 ? '+' : ''}{alert.change}%
                                    </span>
                                    <span className={`text-[10px] font-bold px-1.5 py-0.5 rounded-full uppercase ${
                                        alert.severity === 'high' ? 'bg-red-500/10 text-red-500' : 'bg-orange-500/10 text-orange-500'
                                    }`}>
                                        {alert.severity}
                                    </span>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            </div>
            
            <div className={`mt-6 p-3 rounded-xl flex items-center gap-3 text-xs font-medium border-t ${
                severityColor === 'red' 
                ? 'bg-red-500/5 border-red-200/50 text-red-700 dark:text-red-400' 
                : 'bg-orange-500/5 border-orange-200/50 text-orange-700 dark:text-orange-400'
            }`}>
                <span className="flex-shrink-0 w-2 h-2 rounded-full bg-current animate-pulse"></span>
                We recommend verifying your models trained on earlier versions of this dataset.
            </div>
        </div>
    )
}
