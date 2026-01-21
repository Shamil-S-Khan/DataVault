'use client'

interface LoadingCardProps {
    count?: number
}

export default function LoadingCard({ count = 6 }: LoadingCardProps) {
    return (
        <>
            {[...Array(count)].map((_, i) => (
                <div
                    key={i}
                    className="glass rounded-2xl p-6 shadow-glass border border-gray-200 dark:border-gray-700 animate-pulse"
                    style={{ animationDelay: `${i * 100}ms` }}
                >
                    {/* Title skeleton */}
                    <div className="h-6 bg-gradient-to-r from-gray-200 to-gray-300 dark:from-gray-700 dark:to-gray-600 rounded-lg mb-4 w-3/4"></div>

                    {/* Description skeleton */}
                    <div className="space-y-2 mb-4">
                        <div className="h-4 bg-gradient-to-r from-gray-200 to-gray-300 dark:from-gray-700 dark:to-gray-600 rounded w-full"></div>
                        <div className="h-4 bg-gradient-to-r from-gray-200 to-gray-300 dark:from-gray-700 dark:to-gray-600 rounded w-5/6"></div>
                        <div className="h-4 bg-gradient-to-r from-gray-200 to-gray-300 dark:from-gray-700 dark:to-gray-600 rounded w-2/3"></div>
                    </div>

                    {/* Badges skeleton */}
                    <div className="flex gap-2 mb-4">
                        <div className="h-6 w-20 bg-gradient-to-r from-gray-200 to-gray-300 dark:from-gray-700 dark:to-gray-600 rounded-full"></div>
                        <div className="h-6 w-16 bg-gradient-to-r from-gray-200 to-gray-300 dark:from-gray-700 dark:to-gray-600 rounded-full"></div>
                    </div>

                    {/* Progress bar skeleton */}
                    <div className="h-2 bg-gradient-to-r from-gray-200 to-gray-300 dark:from-gray-700 dark:to-gray-600 rounded-full w-full"></div>
                </div>
            ))}
        </>
    )
}
