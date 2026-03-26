'use client'

import { ClockIcon, ChartBarIcon, ExclamationCircleIcon, ShieldCheckIcon } from '@heroicons/react/24/outline'

interface DriftEvent {
    _id: string
    timestamp: string
    type: string
    score: number
    version: string
    alerts: Array<{
        field: string
        display: string
        change: number
        severity: string
    }>
}

interface DatasetDriftTimelineProps {
    events: DriftEvent[]
}

export default function DatasetDriftTimeline({ events }: DatasetDriftTimelineProps) {
    if (!events || events.length === 0) {
        return (
            <div className="glass rounded-2xl p-8 border border-gray-200 dark:border-gray-700 text-center">
                <div className="w-16 h-16 bg-emerald-50 dark:bg-emerald-900/20 rounded-full flex items-center justify-center mx-auto mb-4">
                    <ShieldCheckIcon className="h-8 w-8 text-emerald-500" />
                </div>
                <h3 className="text-lg font-bold text-gray-900 dark:text-white mb-2">Dataset is Stable</h3>
                <p className="text-gray-500 dark:text-gray-400 max-w-sm mx-auto">
                    No significant distribution drift or major metric changes have been detected across recent versions.
                </p>
            </div>
        )
    }

    return (
        <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700 shadow-xl overflow-hidden">
            <div className="flex items-center gap-3 mb-8">
                <div className="p-2.5 rounded-xl bg-primary-500/10 text-primary-600 dark:text-primary-400">
                    <ChartBarIcon className="h-6 w-6" />
                </div>
                <h3 className="text-xl font-bold text-gray-900 dark:text-white">Drift History</h3>
            </div>

            <div className="relative space-y-8 before:absolute before:inset-0 before:ml-5 before:-translate-x-px before:h-full before:w-0.5 before:bg-gradient-to-b before:from-transparent before:via-gray-200 dark:before:via-gray-800 before:to-transparent">
                {events.map((event, idx) => (
                    <div key={event._id} className="relative flex items-start gap-6 group">
                        {/* Timeline Connector */}
                        <div className={`absolute left-5 -translate-x-1/2 w-3.5 h-3.5 rounded-full border-2 border-white dark:border-gray-900 z-10 transition-transform group-hover:scale-125 ${
                            event.score >= 60 ? 'bg-red-500 animate-pulse' : event.score >= 30 ? 'bg-orange-500' : 'bg-gray-400'
                        }`}></div>

                        <div className="flex-1 ml-4">
                            <div className="flex flex-wrap items-center justify-between gap-2 mb-3">
                                <div>
                                    <span className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-widest block mb-1">
                                        {new Date(event.timestamp).toLocaleString()}
                                    </span>
                                    <h4 className="text-lg font-black text-gray-900 dark:text-white flex items-center gap-2">
                                        Drift Event: {event.version}
                                        <span className={`text-[10px] px-2 py-0.5 rounded uppercase ${
                                            event.score >= 60 ? 'bg-red-500/10 text-red-500' : 'bg-orange-500/10 text-orange-500'
                                        }`}>
                                            Score {event.score}
                                        </span>
                                    </h4>
                                </div>
                            </div>

                            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                                {event.alerts.map((alert, aidx) => (
                                    <div key={aidx} className="flex items-center justify-between p-3 bg-gray-50/50 dark:bg-gray-800/30 rounded-xl border border-gray-100 dark:border-gray-700/50">
                                        <span className="text-xs font-medium text-gray-600 dark:text-gray-400">{alert.display}</span>
                                        <span className={`text-xs font-black ${alert.change > 0 ? 'text-emerald-500' : 'text-red-500'}`}>
                                            {alert.change > 0 ? '+' : ''}{alert.change}%
                                        </span>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>
                ))}
            </div>
            
            <div className="mt-8 p-4 bg-gray-50/50 dark:bg-gray-800/20 border-t border-gray-100 dark:border-gray-700/50">
                <p className="text-[10px] text-gray-500 dark:text-gray-400 flex items-center gap-2 italic">
                    <ClockIcon className="h-3 w-3" />
                    Historical drift monitoring enabled on 2024-01-01. Scores are calculated during synchronization.
                </p>
            </div>
        </div>
    )
}


function ActivityIcon(props: any) {
    return (
        <svg {...props} xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
        </svg>
    )
}

