'use client'

interface BadgeProps {
    children: React.ReactNode
    variant?: 'domain' | 'modality' | 'license' | 'tag' | 'status' | 'trend' | 'success' | 'warning' | 'danger' | 'primary' | 'platform'
    size?: 'sm' | 'md' | 'lg' | 'xs'
    className?: string
}

const variantStyles = {
    domain: 'bg-gradient-to-r from-primary-500 to-primary-600 text-white shadow-md hover:shadow-lg',
    modality: 'bg-gradient-to-r from-secondary-500 to-secondary-600 text-white shadow-md hover:shadow-lg',
    license: 'bg-gradient-to-r from-accent-teal to-secondary-600 text-white shadow-md hover:shadow-lg',
    tag: 'bg-gradient-to-r from-gray-100 to-gray-200 dark:from-gray-700 dark:to-gray-600 text-gray-700 dark:text-gray-300 shadow-sm',
    status: 'bg-gradient-to-r from-orange-500 to-orange-600 text-white shadow-md hover:shadow-lg',
    trend: 'bg-gradient-to-r from-primary-100 to-primary-200 dark:from-primary-900 dark:to-primary-800 text-primary-800 dark:text-primary-200 border border-primary-300 dark:border-primary-700',
    success: 'bg-gradient-to-r from-green-500 to-green-600 text-white shadow-md hover:shadow-lg',
    warning: 'bg-gradient-to-r from-yellow-500 to-yellow-600 text-white shadow-md hover:shadow-lg',
    danger: 'bg-gradient-to-r from-red-500 to-red-600 text-white shadow-md hover:shadow-lg',
    primary: 'bg-gradient-to-r from-blue-500 to-blue-600 text-white shadow-md hover:shadow-lg',
    platform: 'bg-gradient-to-r from-purple-500 to-purple-600 text-white shadow-sm'
}

const sizeStyles = {
    xs: 'px-1.5 py-0.5 text-[10px]',
    sm: 'px-2 py-0.5 text-xs',
    md: 'px-3 py-1 text-sm',
    lg: 'px-4 py-2 text-base'
}

export default function Badge({ children, variant = 'tag', size = 'md', className = '' }: BadgeProps) {
    return (
        <span
            className={`
                inline-flex items-center justify-center
                rounded-full font-medium
                transition-all duration-200
                hover:scale-105
                ${variantStyles[variant]}
                ${sizeStyles[size]}
                ${className}
            `}
        >
            {children}
        </span>
    )
}
