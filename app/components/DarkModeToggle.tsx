'use client'

import { useEffect, useState } from 'react'
import { SunIcon, MoonIcon } from '@heroicons/react/24/outline'

export default function DarkModeToggle() {
    const [darkMode, setDarkMode] = useState(false)

    useEffect(() => {
        // Check localStorage first, then system preference
        const savedTheme = localStorage.getItem('theme')
        const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches

        const shouldBeDark = savedTheme === 'dark' || (!savedTheme && prefersDark)

        if (shouldBeDark) {
            document.documentElement.classList.add('dark')
            setDarkMode(true)
        } else {
            document.documentElement.classList.remove('dark')
            setDarkMode(false)
        }
    }, [])

    const toggleDarkMode = () => {
        if (darkMode) {
            document.documentElement.classList.remove('dark')
            localStorage.setItem('theme', 'light')
        } else {
            document.documentElement.classList.add('dark')
            localStorage.setItem('theme', 'dark')
        }
        setDarkMode(!darkMode)
    }

    return (
        <button
            onClick={toggleDarkMode}
            className="p-2 rounded-lg bg-gray-100 dark:bg-gray-800 hover:bg-gray-200 dark:hover:bg-gray-700 transition-colors"
            aria-label="Toggle dark mode"
        >
            {darkMode ? (
                <SunIcon className="h-5 w-5 text-yellow-500" />
            ) : (
                <MoonIcon className="h-5 w-5 text-gray-700" />
            )}
        </button>
    )
}
