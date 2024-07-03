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
    },
	parserOpts: {
	  headerPattern: /^(\w*)(?:\((.*)\))?: (BI-[0-9]{1,4} .*)$/,
	  headerCorrespondence: ['type', 'scope', 'subject'],
	}
}

export default Configuration;
