'use client'

import { ChartBarIcon } from '@heroicons/react/24/outline'

interface TaskFitness {
    score: number
    max_score: number
    match_rate: number
    reasoning: string[]
}

interface TaskFitnessChartProps {
    taskFitness: Record<string, TaskFitness>
}

export default function TaskFitnessChart({ taskFitness }: TaskFitnessChartProps) {
    if (!taskFitness || Object.keys(taskFitness).length === 0) {
        return null
    }

    const getScoreColor = (score: number) => {
        if (score >= 8) return 'bg-emerald-500'
        if (score >= 6) return 'bg-yellow-500'
        if (score >= 4) return 'bg-orange-500'
        return 'bg-red-500'
    }

    const getScoreTextColor = (score: number) => {
        if (score >= 8) return 'text-emerald-500'
        if (score >= 6) return 'text-yellow-500'
        if (score >= 4) return 'text-orange-500'
        return 'text-red-500'
    }

    return (
        <div className="glass rounded-2xl p-6 border border-gray-200 dark:border-gray-700 shadow-xl mb-8">
            <div className="flex items-center gap-3 mb-6">
                <div className="p-2.5 rounded-xl bg-primary-500/10 text-primary-600 dark:text-primary-400">
                    <ChartBarIcon className="h-6 w-6" />
                </div>
                <h3 className="text-xl font-bold text-gray-900 dark:text-white">Task Fitness Analysis</h3>
            </div>

            <div className="space-y-6">
                {Object.entries(taskFitness).map(([task, data]) => (
                    <div key={task} className="group">
                        <div className="flex items-center justify-between mb-2">
                            <span className="text-sm font-bold text-gray-700 dark:text-gray-300 group-hover:text-primary-500 transition-colors">
                                {task}
                            </span>
                            <div className="flex items-center gap-2">
                                <span className={`text-sm font-black ${getScoreTextColor(data.score)}`}>
                                    {data.match_rate}%
                                </span>
                                <span className="text-[10px] text-gray-400 uppercase font-black tracking-tighter">Match</span>
                            </div>
                        </div>
                        
                        <div className="relative h-3 w-full bg-gray-100 dark:bg-gray-800 rounded-full overflow-hidden border border-gray-200/50 dark:border-gray-700/50">
                            {/* Background track indicator */}
                            <div className="absolute inset-y-0 left-0 w-full flex justify-between px-1 opacity-10">
                                {[...Array(10)].map((_, i) => (
                                    <div key={i} className="w-px h-full bg-gray-400"></div>
                                ))}
                            </div>
                            
                            {/* Progress bar */}
                            <div 
                                className={`h-full ${getScoreColor(data.score)} transition-all duration-1000 ease-out shadow-[0_0_10px_rgba(0,0,0,0.1)]`}
                                style={{ width: `${data.match_rate}%` }}
                            >
                                <div className="w-full h-full bg-gradient-to-r from-white/20 to-transparent"></div>
                            </div>
                        </div>

                        {/* Reasoning highlights */}
                        <div className="mt-2 flex flex-wrap gap-2">
                            {data.reasoning.map((r, i) => (
                                <span key={i} className="text-[10px] bg-gray-50 dark:bg-gray-800/50 text-gray-500 dark:text-gray-400 px-2 py-0.5 rounded-full border border-gray-100 dark:border-gray-700/50">
                                    {r}
                                </span>
                            ))}
                        </div>
                    </div>
                ))}
            </div>

            <div className="mt-8 p-4 rounded-xl bg-primary-50 dark:bg-primary-900/10 border border-primary-100 dark:border-primary-800/50">
                <p className="text-xs text-primary-700 dark:text-primary-300 leading-relaxed italic">
                    Fitness scores represent the mathematical alignment between the dataset&apos;s features, modality, and the specific requirements of the ML task. A score &gt;75% indicates highly reliable suitability.
                </p>
            </div>
        </div>
    )
}
