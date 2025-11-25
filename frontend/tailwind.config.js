/** @type {import('tailwindcss').Config} */
export default {
  darkMode: "class",
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  corePlugins: {
    preflight: false, // Disable preflight to avoid conflict with Ant Design
  },
  theme: {
    extend: {
      colors: {
        brand: {
          50: '#f0f9ff',
          100: '#e0f2fe',
          500: '#0ea5e9', // Sky blue
          600: '#0284c7',
          900: '#0c4a6e',
          dark: '#0f172a', // Slate 900
          darker: '#020617', // Slate 950
        }
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
        mono: ['Fira Code', 'monospace'],
      },
      backgroundImage: {
        'grid-pattern': "linear-gradient(to right, #1e293b 1px, transparent 1px), linear-gradient(to bottom, #1e293b 1px, transparent 1px)",
        'circuit-pattern': "radial-gradient(#334155 1px, transparent 1px)",
      },
      animation: {
        'float': 'float 6s ease-in-out infinite',
        'type': 'type 2s steps(40, end)',
        'bar-grow': 'grow 1.5s ease-out forwards',
        'pulse-slow': 'pulse 4s cubic-bezier(0.4, 0, 0.6, 1) infinite',
      },
      keyframes: {
        float: {
          '0%, 100%': { transform: 'translateY(0)' },
          '50%': { transform: 'translateY(-10px)' },
        },
        grow: {
          '0%': { height: '0%' },
          '100%': { height: 'var(--h)' },
        }
      }
    },
  },
  plugins: [],
}
