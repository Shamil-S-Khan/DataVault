'use client'

interface EmptyStateProps {
    icon?: string
    title: string
    description?: string
    action?: {
        label: string
        onClick: () => void
    }
}

export default function EmptyState({ icon = '📭', title, description, action }: EmptyStateProps) {
    return (
        <div className="flex flex-col items-center justify-center py-16 px-4">
            <div className="w-24 h-24 mb-6 rounded-full bg-gradient-to-br from-gray-100 to-gray-200 dark:from-gray-800 dark:to-gray-700 flex items-center justify-center shadow-lg">
                <span className="text-5xl">{icon}</span>
            </div>
            <h3 className="text-2xl font-bold text-gray-900 dark:text-white mb-2">
                {title}
            </h3>
            {description && (
                <p className="text-gray-600 dark:text-gray-400 text-center max-w-md mb-6">
                    {description}
                </p>
            )}
            {action && (
                <button
                    onClick={action.onClick}
                    className="px-6 py-3 bg-gradient-to-r from-primary-500 to-primary-600 text-white rounded-xl hover:from-primary-600 hover:to-primary-700 transition-all duration-200 font-medium shadow-lg hover:shadow-xl hover:scale-105"
                >
                    {action.label}
                </button>
            )}
        </div>
    )
}
