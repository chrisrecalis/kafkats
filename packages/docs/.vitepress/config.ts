import { defineConfig } from 'vitepress'

export default defineConfig({
	title: 'kafkats',
	description: 'Pure-protocol Kafka client and streams library for TypeScript',
	base: '/kafkats/',

	head: [
		['link', { rel: 'icon', href: '/favicon.ico' }],
		['meta', { property: 'og:type', content: 'website' }],
		['meta', { property: 'og:title', content: 'kafkats' }],
		[
			'meta',
			{
				property: 'og:description',
				content: 'Pure-protocol Kafka client and streams library for TypeScript',
			},
		],
		['meta', { name: 'twitter:card', content: 'summary_large_image' }],
	],

	themeConfig: {
		logo: '/logo.svg',

		nav: [
			{ text: 'Guide', link: '/guide/' },
			{
				text: 'Packages',
				items: [
					{ text: '@kafkats/client', link: '/client/' },
					{ text: '@kafkats/flow', link: '/flow/' },
					{ text: '@kafkats/flow-codec-zod', link: '/flow-codec-zod/' },
					{ text: '@kafkats/flow-state-lmdb', link: '/flow-state-lmdb/' },
				],
			},
			{ text: 'Examples', link: '/examples/' },
		],

		sidebar: {
			'/guide/': [
				{
					text: 'Introduction',
					items: [
						{ text: 'Getting Started', link: '/guide/' },
						{ text: 'Installation', link: '/guide/installation' },
						{ text: 'Quick Start', link: '/guide/quick-start' },
						{ text: 'Core Concepts', link: '/guide/concepts' },
					],
				},
			],

			'/client/': [
				{
					text: '@kafkats/client',
					items: [
						{ text: 'Overview', link: '/client/' },
						{ text: 'Getting Started', link: '/client/getting-started' },
						{ text: 'Configuration', link: '/client/configuration' },
					],
				},
				{
					text: 'Producer',
					items: [
						{ text: 'Producer API', link: '/client/producer' },
						{ text: 'Transactions', link: '/client/transactions' },
					],
				},
				{
					text: 'Consumer',
					items: [
						{ text: 'Consumer API', link: '/client/consumer' },
						{ text: 'ShareConsumer (experimental)', link: '/client/share-consumer' },
					],
				},
				{
					text: 'Admin',
					items: [{ text: 'Admin API', link: '/client/admin' }],
				},
				{
					text: 'Fundamentals',
					items: [
						{ text: 'Compression', link: '/client/compression' },
						{ text: 'Codecs', link: '/client/codecs' },
						{ text: 'Authentication', link: '/client/authentication' },
						{ text: 'Error Handling', link: '/client/errors' },
					],
				},
				{
					text: 'Advanced',
					collapsed: true,
					items: [
						{ text: 'Cluster API', link: '/client/advanced/cluster' },
						{ text: 'Broker API', link: '/client/advanced/broker' },
						{ text: 'Protocol', link: '/client/advanced/protocol' },
					],
				},
			],

			'/flow/': [
				{
					text: '@kafkats/flow',
					items: [
						{ text: 'Overview', link: '/flow/' },
						{ text: 'Getting Started', link: '/flow/getting-started' },
					],
				},
				{
					text: 'Core APIs',
					items: [
						{ text: 'KStream', link: '/flow/streams' },
						{ text: 'KTable', link: '/flow/tables' },
						{ text: 'Operations', link: '/flow/operations' },
					],
				},
				{
					text: 'Stateful Processing',
					items: [
						{ text: 'Windowing', link: '/flow/windowing' },
						{ text: 'Aggregations', link: '/flow/aggregations' },
						{ text: 'Joins', link: '/flow/joins' },
						{ text: 'State Stores', link: '/flow/state-stores' },
					],
				},
				{
					text: 'Utilities',
					items: [
						{ text: 'Codecs', link: '/flow/codecs' },
						{ text: 'Testing', link: '/flow/testing' },
					],
				},
			],

			'/flow-codec-zod/': [
				{
					text: '@kafkats/flow-codec-zod',
					items: [
						{ text: 'Overview', link: '/flow-codec-zod/' },
						{ text: 'Usage', link: '/flow-codec-zod/usage' },
					],
				},
			],

			'/flow-state-lmdb/': [
				{
					text: '@kafkats/flow-state-lmdb',
					items: [
						{ text: 'Overview', link: '/flow-state-lmdb/' },
						{ text: 'Configuration', link: '/flow-state-lmdb/configuration' },
						{ text: 'Store Types', link: '/flow-state-lmdb/stores' },
					],
				},
			],

			'/examples/': [
				{
					text: 'Examples',
					items: [
						{ text: 'Overview', link: '/examples/' },
						{ text: 'Simple Producer', link: '/examples/simple-producer' },
						{ text: 'Consumer Group', link: '/examples/consumer-group' },
						{ text: 'Stream Processing', link: '/examples/stream-processing' },
						{ text: 'Word Count', link: '/examples/word-count' },
					],
				},
			],
		},

		socialLinks: [{ icon: 'github', link: 'https://github.com/chrisrecalis/kafkats' }],

		footer: {
			message: 'Released under the MIT License.',
			copyright: 'Copyright 2024-present kafkats contributors',
		},

		search: {
			provider: 'local',
		},

		editLink: {
			pattern: 'https://github.com/chrisrecalis/kafkats/edit/main/packages/docs/:path',
			text: 'Edit this page on GitHub',
		},

		outline: {
			level: [2, 3],
		},
	},

	markdown: {
		theme: {
			light: 'github-light',
			dark: 'github-dark',
		},
		lineNumbers: true,
	},

	cleanUrls: true,
	lastUpdated: true,
})
