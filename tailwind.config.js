/** @type {import('tailwindcss').Config} */
module.exports = {
    content: [
        './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
        './src/components/**/*.{js,ts,jsx,tsx,mdx}',
        './src/app/**/*.{js,ts,jsx,tsx,mdx}',
        './app/**/*.{js,ts,jsx,tsx,mdx}',
    ],
    darkMode: 'class',
    theme: {
        extend: {
            colors: {
                primary: {
                    50: '#faf5ff',
                    100: '#f3e8ff',
                    200: '#e9d5ff',
                    300: '#d8b4fe',
                    400: '#c084fc',
                    500: '#a855f7',
                    600: '#9333ea',
                    700: '#7e22ce',
                    800: '#6b21a8',
                    900: '#581c87',
                    950: '#3b0764',
                },
                secondary: {
                    50: '#ecfdf5',
                    100: '#d1fae5',
                    200: '#a7f3d0',
                    300: '#6ee7b7',
                    400: '#34d399',
                    500: '#10b981',
                    600: '#059669',
                    700: '#047857',
                    800: '#065f46',
                    900: '#064e3b',
                    950: '#022c22',
                },
                accent: {
                    purple: '#a855f7',
                    pink: '#ec4899',
                    orange: '#fb923c',
                    teal: '#14b8a6',
                    cyan: '#06b6d4',
                    indigo: '#6366f1',
                },
                glass: {
                    light: 'rgba(255, 255, 255, 0.7)',
                    dark: 'rgba(17, 25, 40, 0.75)',
                },
            },
            fontFamily: {
                sans: ['Inter', '-apple-system', 'BlinkMacSystemFont', 'system-ui', 'sans-serif'],
                mono: ['JetBrains Mono', 'SF Mono', 'Courier New', 'monospace'],
            },
            animation: {
                'fade-in': 'fadeIn 0.5s ease-in-out',
                'fade-in-up': 'fadeInUp 0.6s ease-out',
                'slide-up': 'slideUp 0.5s ease-out',
                'slide-down': 'slideDown 0.4s ease-out',
                'scale-in': 'scaleIn 0.3s ease-out',
                'scale-up': 'scaleUp 0.4s cubic-bezier(0.16, 1, 0.3, 1)',
                'float': 'float 3s ease-in-out infinite',
                'float-slow': 'float 6s ease-in-out infinite',
                'shimmer': 'shimmer 2s infinite',
                'gradient-shift': 'gradient-shift 15s ease infinite',
                'pulse-glow': 'pulse-glow 2s ease-in-out infinite',
                'glow': 'glow 2s ease-in-out infinite alternate',
                'bounce-subtle': 'bounceSubtle 1s ease-in-out',
                'spin-slow': 'spin 3s linear infinite',
            },
            keyframes: {
                fadeIn: {
                    '0%': { opacity: '0', transform: 'translateY(10px)' },
                    '100%': { opacity: '1', transform: 'translateY(0)' },
                },
                fadeInUp: {
                    '0%': { opacity: '0', transform: 'translateY(20px)' },
                    '100%': { opacity: '1', transform: 'translateY(0)' },
                },
                slideUp: {
                    '0%': { opacity: '0', transform: 'translateY(30px)' },
                    '100%': { opacity: '1', transform: 'translateY(0)' },
                },
                slideDown: {
                    '0%': { opacity: '0', transform: 'translateY(-20px)' },
                    '100%': { opacity: '1', transform: 'translateY(0)' },
                },
                scaleIn: {
                    '0%': { opacity: '0', transform: 'scale(0.95)' },
                    '100%': { opacity: '1', transform: 'scale(1)' },
                },
                scaleUp: {
                    '0%': { opacity: '0', transform: 'scale(0.9)' },
                    '100%': { opacity: '1', transform: 'scale(1)' },
                },
                float: {
                    '0%, 100%': { transform: 'translateY(0px)' },
                    '50%': { transform: 'translateY(-10px)' },
                },
                shimmer: {
                    '0%': { backgroundPosition: '-200% 0' },
                    '100%': { backgroundPosition: '200% 0' },
                },
                'gradient-shift': {
                    '0%': { backgroundPosition: '0% 50%' },
                    '50%': { backgroundPosition: '100% 50%' },
                    '100%': { backgroundPosition: '0% 50%' },
                },
                'pulse-glow': {
                    '0%, 100%': {
                        boxShadow: '0 0 5px rgba(168, 85, 247, 0.5), 0 0 10px rgba(168, 85, 247, 0.3)',
                    },
                    '50%': {
                        boxShadow: '0 0 10px rgba(168, 85, 247, 0.8), 0 0 20px rgba(168, 85, 247, 0.5), 0 0 30px rgba(168, 85, 247, 0.3)',
                    },
                },
                glow: {
                    '0%': { boxShadow: '0 0 20px rgba(168, 85, 247, 0.3)' },
                    '100%': { boxShadow: '0 0 30px rgba(168, 85, 247, 0.6), 0 0 40px rgba(168, 85, 247, 0.4)' },
                },
                bounceSubtle: {
                    '0%, 100%': { transform: 'translateY(0)' },
                    '50%': { transform: 'translateY(-5px)' },
                },
            },
            boxShadow: {
                'glow': '0 0 20px rgba(168, 85, 247, 0.4)',
                'glow-lg': '0 0 30px rgba(168, 85, 247, 0.5)',
                'glow-xl': '0 0 40px rgba(168, 85, 247, 0.6)',
                'glass': '0 8px 32px 0 rgba(31, 38, 135, 0.15)',
                'glass-lg': '0 12px 40px 0 rgba(31, 38, 135, 0.2)',
                'lift': '0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04)',
                'lift-lg': '0 25px 50px -12px rgba(0, 0, 0, 0.25)',
                'inner-glow': 'inset 0 0 20px rgba(168, 85, 247, 0.1)',
                'floating': '0 10px 30px -5px rgba(0, 0, 0, 0.1), 0 0 20px rgba(168, 85, 247, 0.2)',
            },
            backdropBlur: {
                xs: '2px',
            },
            borderRadius: {
                '4xl': '2rem',
            },
        },
    },
    plugins: [],
}
