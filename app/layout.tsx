import type { Metadata, Viewport } from 'next'
import { Inter } from 'next/font/google'
import './globals.css'

const inter = Inter({ 
    subsets: ['latin'],
    display: 'swap',
    preload: true,
    variable: '--font-inter'
})

export const viewport: Viewport = {
    width: 'device-width',
    initialScale: 1,
    maximumScale: 5,
    userScalable: true,
    themeColor: [
        { media: '(prefers-color-scheme: light)', color: '#ffffff' },
        { media: '(prefers-color-scheme: dark)', color: '#0f172a' }
    ]
}

export const metadata: Metadata = {
    title: 'DataVault - Dataset Discovery Platform',
    description: 'Discover, track, and analyze emerging datasets across ML research and industry',
    keywords: ['datasets', 'machine learning', 'AI', 'data science', 'trend analysis'],
}

export default function RootLayout({
    children,
}: {
    children: React.ReactNode
}) {
    return (
        <html lang="en">
            <body className={inter.className}>
                <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100 dark:from-slate-900 dark:to-slate-800">
                    {children}
                </div>
            </body>
        </html>
    )
}
