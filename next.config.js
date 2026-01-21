/** @type {import('next').NextConfig} */
const nextConfig = {
    reactStrictMode: true,
    swcMinify: true,
    compress: true,
    poweredByHeader: false,
    images: {
        domains: ['huggingface.co', 'kaggle.com', 'github.com'],
        formats: ['image/avif', 'image/webp'],
        minimumCacheTTL: 60,
    },
    env: {
        NEXT_PUBLIC_API_URL: process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000',
    },
    // Optimize bundle size
    experimental: {
        optimizePackageImports: ['@heroicons/react'],
    },
    // Enable aggressive code splitting
    webpack: (config) => {
        config.optimization = {
            ...config.optimization,
            splitChunks: {
                chunks: 'all',
                cacheGroups: {
                    default: false,
                    vendors: false,
                    // Vendor chunk
                    vendor: {
                        name: 'vendor',
                        chunks: 'all',
                        test: /node_modules/,
                        priority: 20
                    },
                    // Common chunk
                    common: {
                        name: 'common',
                        minChunks: 2,
                        chunks: 'all',
                        priority: 10,
                        reuseExistingChunk: true,
                        enforce: true
                    }
                }
            }
        }
        return config
    },
}

module.exports = nextConfig
