'use client'

import { useState, useEffect } from 'react'
import { ChartBarIcon, Bars3Icon, XMarkIcon } from '@heroicons/react/24/outline'
import DarkModeToggle from '../components/DarkModeToggle'

interface LayoutHeaderProps {
    showNav?: boolean
}

export default function LayoutHeader({ showNav = true }: LayoutHeaderProps) {
    const [isScrolled, setIsScrolled] = useState(false)
    const [mobileMenuOpen, setMobileMenuOpen] = useState(false)

    useEffect(() => {
        const handleScroll = () => {
            setIsScrolled(window.scrollY > 10)
        }
        window.addEventListener('scroll', handleScroll)
        return () => window.removeEventListener('scroll', handleScroll)
    }, [])

    return (
        <>
            <header
                className={`fixed top-0 left-0 right-0 z-50 transition-all duration-300 animate-slide-down ${
                    isScrolled
                        ? 'backdrop-blur-xl bg-white/80 dark:bg-gray-900/80 shadow-glass'
                        : 'bg-white/70 dark:bg-gray-900/70 backdrop-blur-md shadow-sm'
                } border-b border-gray-200/50 dark:border-gray-800/50`}
            >
                <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-3 sm:py-4">
                    <div className="flex items-center justify-between">
                        <div
                            className="flex items-center gap-2 sm:gap-3 cursor-pointer hover:scale-105 active:scale-95 transition-transform"
                            onClick={() => window.location.href = '/'}
                        >
                            <div className="p-1.5 sm:p-2 rounded-xl bg-gradient-to-br from-primary-500 to-secondary-500 shadow-glow hover:rotate-6 transition-transform">
                                <ChartBarIcon className="h-6 w-6 sm:h-8 sm:w-8 text-white" />
                            </div>
                            <div>
                                <h1 className="text-xl sm:text-2xl md:text-3xl font-bold gradient-text">
                                    DataVault
                                </h1>
                                <p className="text-[10px] sm:text-xs text-gray-600 dark:text-gray-400 font-medium hidden sm:block">
                                    Discover Emerging Datasets
                                </p>
                            </div>
                        </div>
                        <div className="flex items-center gap-2 sm:gap-4">
                            {showNav && (
                                <>
                                    <nav className="hidden md:flex items-center gap-6">
                                        <NavLink href="/">Home</NavLink>
                                        <NavLink href="/explore">Explore</NavLink>
                                    </nav>
                                    <button
                                        onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
                                        className="md:hidden p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors"
                                        aria-label="Toggle menu"
                                    >
                                        {mobileMenuOpen ? (
                                            <XMarkIcon className="h-6 w-6 text-gray-700 dark:text-gray-300" />
                                        ) : (
                                            <Bars3Icon className="h-6 w-6 text-gray-700 dark:text-gray-300" />
                                        )}
                                    </button>
                                </>
                            )}
                            <div className="animate-scale-in">
                                <DarkModeToggle />
                            </div>
                        </div>
                    </div>
                    {/* Mobile Menu */}
                    {showNav && mobileMenuOpen && (
                        <nav className="md:hidden mt-4 pb-4 border-t border-gray-200 dark:border-gray-700 pt-4 animate-slide-down">
                            <div className="flex flex-col gap-3">
                                <a
                                    href="/"
                                    className="px-4 py-2 text-sm font-semibold text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-lg transition-colors"
                                    onClick={() => setMobileMenuOpen(false)}
                                >
                                    Home
                                </a>
                                <a
                                    href="/explore"
                                    className="px-4 py-2 text-sm font-semibold text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-lg transition-colors"
                                    onClick={() => setMobileMenuOpen(false)}
                                >
                                    Explore
                                </a>
                            </div>
                        </nav>
                    )}
                </div>
            </header>
            {/* Spacer to offset fixed header height so content isn't covered */}
            <div className="h-20 sm:h-24 w-full" aria-hidden="true" />
        </>
    )
}

// Animated NavLink Component
function NavLink({ href, children }: { href: string; children: React.ReactNode }) {
    const [isHovered, setIsHovered] = useState(false)

    return (
        <a
            href={href}
            className="relative text-sm font-semibold text-gray-700 dark:text-gray-300 hover:-translate-y-0.5 transition-all"
            onMouseEnter={() => setIsHovered(true)}
            onMouseLeave={() => setIsHovered(false)}
        >
            {children}
            {isHovered && (
                <div className="absolute -bottom-1 left-0 right-0 h-0.5 bg-gradient-to-r from-primary-500 to-secondary-500 rounded-full animate-scale-in" />
            )}
        </a>
    )
}
