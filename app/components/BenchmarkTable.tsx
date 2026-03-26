'use client'

import { TrophyIcon, BookOpenIcon, ArrowTopRightOnSquareIcon } from '@heroicons/react/24/outline'

interface Benchmark {
    task: string
    metric: string
    sota_value: string | number
    paper_url?: string
    model_name?: string
}

interface BenchmarkTableProps {
    benchmarks: Benchmark[]
}

export default function BenchmarkTable({ benchmarks }: BenchmarkTableProps) {
    if (!benchmarks || benchmarks.length === 0) {
        return (
            <div className="glass rounded-2xl p-8 border border-gray-200 dark:border-gray-700 text-center">
                <div className="w-16 h-16 bg-gray-100 dark:bg-gray-800 rounded-full flex items-center justify-center mx-auto mb-4">
                    <TrophyIcon className="h-8 w-8 text-gray-400" />
                </div>
                <h3 className="text-lg font-bold text-gray-900 dark:text-white mb-2">No Benchmarks Available</h3>
                <p className="text-gray-500 dark:text-gray-400 max-w-sm mx-auto">
                    We haven't indexed any official benchmarks or SOTA results for this dataset from Papers With Code yet.
                </p>
            </div>
        )
    }

    return (
        <div className="glass rounded-2xl border border-gray-200 dark:border-gray-700 overflow-hidden shadow-xl">
            <div className="p-6 border-b border-gray-200 dark:border-gray-700 bg-white/50 dark:bg-gray-900/50 flex items-center justify-between">
                <div className="flex items-center gap-3">
                    <TrophyIcon className="h-6 w-6 text-yellow-500" />
                    <h3 className="text-xl font-bold text-gray-900 dark:text-white">Official Benchmarks & SOTA</h3>
                </div>
                <div className="flex items-center gap-2">
                    <span className="text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-widest bg-gray-100 dark:bg-gray-800 px-3 py-1 rounded-full">
                        {benchmarks.length} Results
                    </span>
                </div>
            </div>

            <div className="overflow-x-auto">
                <table className="w-full text-left border-collapse">
                    <thead>
                        <tr className="bg-gray-50/50 dark:bg-gray-800/20">
                            <th className="px-6 py-4 text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Task</th>
                            <th className="px-6 py-4 text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">SOTA Metric</th>
                            <th className="px-6 py-4 text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider">Best Result</th>
                            <th className="px-6 py-4 text-xs font-bold text-gray-500 dark:text-gray-400 uppercase tracking-wider text-right">Reference</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                        {benchmarks.map((bench, idx) => (
                            <tr key={idx} className="hover:bg-primary-500/5 transition-colors">
                                <td className="px-6 py-5">
                                    <span className="font-bold text-gray-900 dark:text-white">{bench.task}</span>
                                </td>
                                <td className="px-6 py-5">
                                    <span className="text-sm text-gray-600 dark:text-gray-400 bg-gray-100 dark:bg-gray-800 px-2 py-0.5 rounded">
                                        {bench.metric}
                                    </span>
                                </td>
                                <td className="px-6 py-5">
                                    <div className="flex flex-col">
                                        <span className="text-lg font-black text-primary-600 dark:text-primary-400">
                                            {bench.sota_value}
                                        </span>
                                        {bench.model_name && (
                                            <span className="text-[10px] font-medium text-gray-500 dark:text-gray-400 truncate max-w-[150px]">
                                                by {bench.model_name}
                                            </span>
                                        )}
                                    </div>
                                </td>
                                <td className="px-6 py-5 text-right">
                                    {bench.paper_url ? (
                                        <a 
                                            href={bench.paper_url} 
                                            target="_blank" 
                                            rel="noopener noreferrer"
                                            className="inline-flex items-center gap-1.5 text-xs font-bold text-primary-600 dark:text-primary-400 hover:text-primary-700 dark:hover:text-primary-300 transition-colors"
                                        >
                                            <BookOpenIcon className="h-4 w-4" />
                                            Paper
                                            <ArrowTopRightOnSquareIcon className="h-3 w-3" />
                                        </a>
                                    ) : (
                                        <span className="text-gray-400 text-xs">No paper linked</span>
                                    )}
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
            
            <div className="p-4 bg-yellow-50/50 dark:bg-yellow-900/10 border-t border-yellow-100 dark:border-yellow-900/20">
                <p className="text-[10px] text-yellow-700 dark:text-yellow-400 flex items-center gap-2">
                    <span className="flex-shrink-0 w-1.5 h-1.5 rounded-full bg-yellow-500 animate-pulse"></span>
                    Verified via Papers With Code. SOTA results are periodically synchronized.
                </p>
            </div>
        </div>
    )
}
