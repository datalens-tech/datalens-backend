import {
	RuleConfigSeverity,
} from '@commitlint/types';

const Configuration = {
    extends: ['@commitlint/config-conventional'],
    rules: {
        'type-enum': [
			RuleConfigSeverity.Error,
			'always',
			[
				'breaking-change',
				'new-feature',
				'bug-fix',
				'sec',
				'deprecation',
				'dev',
				'tests',
				'ci',
				'chore',
				'build',
				'docs',
			],
		],
		'scope-enum': [
			RuleConfigSeverity.Error,
			'always',
			[
				'charts',
				'connectors',
				'navigation',
				'general',
				'dashboards',
				'datasets',
				'auth',
				'optimization',
				'role-model',
				'docs',
				'formula',
			],
		],
		'subject-case': [
			RuleConfigSeverity.Error,
			'never',
			['start-case', 'pascal-case', 'upper-case'],
		],
		'subject-prefix': [
			RuleConfigSeverity.Error,
			'always',
		],

    },
	plugins: [
		{
		  rules: {
			'subject-prefix': ({subject}) => {
				const PREFIX = 'BI-';
				return [
					subject.startsWith(PREFIX),
					`Subject must start with ${PREFIX}`,
				]
			},
		  },
		},
	],
}

export default Configuration;
