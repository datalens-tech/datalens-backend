from __future__ import annotations

import random as base_random
from collections import namedtuple

import pytest


DirectMembership = namedtuple('DirectMembership', ('group', 'subject'))
# NOTE: `reason` is a *path* tuple, e.g. `(group, G2, G3, G4, subject)`
EffectiveMembership = namedtuple('EffectiveMembership', ('group', 'subject', 'reason'))


def pairs(iterable):
    """ Generator of unpadded pairs tuples from another iterable """
    iterable = iter(iterable)
    try:
        v1 = next(iterable)
    except StopIteration:  # empty iterable
        return
    for v2 in iterable:
        yield v1, v2
        v1 = v2


class Worker:

    log_enable = True

    def __init__(self):
        self.direct_memberships = set()
        self.effective_memberships = set()

    def log(self, msg, *args):
        if not self.log_enable:
            return
        print((msg % args) + '\n')

    def get_subject_direct_groups(self, subject):
        return [
            item.group
            for item in self.direct_memberships
            if item.subject == subject]

    def get_subject_effective_group_memberships(self, subject):
        ''' Note: use `item.group` '''
        return [
            item
            for item in self.effective_memberships
            if item.subject == subject]

    def get_direct_members(self, group):
        ''' Note: use `item.subject` '''
        return [
            item.subject
            for item in self.direct_memberships
            if item.group == group]

    def get_group_effective_subject_memberships(self, group):
        ''' Note: use `item.subject` '''
        return [
            item
            for item in self.effective_memberships
            if item.group == group]

    def add_effective_memberships(self, effectives):
        assert all(isinstance(membership, EffectiveMembership) for membership in effectives)
        effectives = set(effectives)
        assert not effectives & self.effective_memberships, "effectives to be added should not exist"
        self.effective_memberships.update(effectives)

    def del_effective_memberships(self, rm_effectives):
        assert all(isinstance(membership, EffectiveMembership) for membership in rm_effectives)
        rm_effectives = set(rm_effectives)
        assert not rm_effectives - self.effective_memberships, (
            "all effectives to be removed should've existed")
        self.effective_memberships.difference_update(rm_effectives)

    def group_add_subject(self, group, subject, is_user='auto'):
        if is_user == 'auto':
            is_user = subject.startswith('user')

        edge = DirectMembership(subject=subject, group=group)
        if edge in self.direct_memberships:
            return []
        self.direct_memberships.add(edge)

        base_effective = EffectiveMembership(
            group=group, subject=subject, reason=(group, subject))

        if is_user:
            # Optimized case when there's no need to process the subject's subsubjects.
            self_effective_group_memberships = self.get_subject_effective_group_memberships(group)
            effectives = (
                [base_effective] +
                # subject belongs to group => subject belongs to supgroup
                [
                    EffectiveMembership(
                        group=supgroup_item.group, subject=subject,
                        reason=supgroup_item + (subject,))
                    for supgroup_item in self_effective_group_memberships]
            )

        elif group == subject:
            # Allow explicit self-links but do not really process them.
            effectives = [base_effective]
            # effectives = []

        else:
            self_effective_group_memberships = self.get_subject_effective_group_memberships(group)
            supgroup_items = (
                [base_effective] +
                self_effective_group_memberships)

            other_effective_memberships = self.get_group_effective_subject_memberships(subject)
            subject_items = (
                [base_effective] +
                other_effective_memberships)

            effectives = [
                EffectiveMembership(
                    group=supgroup_item.group, subject=subject_item.subject,
                    reason=supgroup_item.reason + subject_item.reason)
                for supgroup_item in supgroup_items
                for subject_item in subject_items
                # if supgroup_item.group != subject_item.subject
            ]

        self.log(
            'group_add_subject(%r <- %r): is_user=%r, new effective memberships: %r',
            group, subject, is_user, effectives)
        self.add_effective_memberships(effectives)
        return effectives

    def group_del_subject(self, group, subject, force=False, is_user='auto', by_reason=True):
        ''' Remove 'subject' from 'group' '''
        if is_user == 'auto':
            is_user = subject.startswith('user')

        edge = DirectMembership(subject=subject, group=group)
        if edge not in self.direct_memberships:
            if not force:
                raise ValueError('Subject %r does not belong directly to group %r' % (
                    subject, group))
            return []

        self.direct_memberships.remove(edge)

        base_effective = EffectiveMembership(
            group=group, subject=subject, reason=(group, subject))

        if is_user:

            if by_reason:
                rm_effectives = [
                    item for item in self.effective_memberships
                    if item.subject == subject and
                    item.reason[-2:] == (group, subject)]

            else:
                new_directs = self.get_subject_direct_groups(subject)
                new_effectives = (
                    [
                        EffectiveMembership(
                            group=direct_group,
                            subject=subject,
                            reason=(direct_group, subject),
                        )
                        for direct_group in new_directs
                    ] +
                    [
                        EffectiveMembership(
                            group=supitem.group,
                            subject=subject,
                            reason=supitem.reason + (subject,),
                        )
                        for item in new_directs
                        for supitem in self.get_subject_effective_group_memberships(item.group)
                    ]
                )
                rm_effectives = self.get_subject_effective_group_memberships(subject) - new_effectives

        elif group == subject:
            rm_effectives = [base_effective]
            # rm_effectives = []

        else:
            if by_reason:
                rm_effectives = [
                    item for item in self.effective_memberships
                    if
                    # subject in item.reason and
                    (group, subject) in pairs(item.reason)
                ]
            else:
                rm_effectives = set()
                for subject_membership in self.get_group_effective_subject_memberships(subject):
                    raise Exception("TODO", subject_membership)
                raise Exception("TODO")

        self.log(
            ('group_del_subject(%r <- %r): by_reason=%r, is_user=%r,'
             ' remove effective memberships: %r'),
            group, subject, by_reason, is_user, rm_effectives)
        self.del_effective_memberships(rm_effectives)
        return rm_effectives

    def run_actions(self, actions, with_check_the_world=True):
        for action in actions:
            func, args = action[0], action[1:]
            if isinstance(func, str):
                func = getattr(self, func)
            func(*args)
            if with_check_the_world:
                self.check_the_world()

    def check_the_world(self):
        # NOTE: not checking the 'reason' part.
        group_to_direct_subjects = {}
        for item in self.direct_memberships:
            if item.group == item.subject:  # ignore the self-links here
                continue
            group_to_direct_subjects.setdefault(item.group, []).append(item.subject)

        group_to_effective_subjects_fullbuilt = self.generate_all_effective_group_subjects(
            group_to_direct_subjects)

        group_to_effective_subjects_maintained = {}
        for item in self.effective_memberships:
            # if item.group == item.subject:  # ignore the self-links here too
            #     continue
            group_to_effective_subjects_maintained.setdefault(item.group, set()).add(item.subject)

        if group_to_effective_subjects_fullbuilt != group_to_effective_subjects_maintained:
            try:
                from pyaux.madness import p_datadiff
                p_datadiff(
                    group_to_effective_subjects_fullbuilt,
                    group_to_effective_subjects_maintained)
                print(' ^^^ correct -> incorrect diff')
            except Exception:
                pass

            self.generate_all_effective_group_subjects(group_to_direct_subjects)

            raise ValueError("World diverged", dict(
                current=group_to_effective_subjects_maintained,
                expected=group_to_effective_subjects_fullbuilt))

    def generate_all_effective_group_subjects(self, group_to_subjects, raise_on_loop=False):
        group_to_subjects_memo = {}

        def walk_group_to_subjects(group, parents=(), root=None):
            result = group_to_subjects_memo.get(group)
            if result is not None:
                return result

            group_path = parents + (group,)
            result = []

            # Recurse only if we haven't looped a loop.
            looped = False
            if group in parents:
                if raise_on_loop:
                    raise ValueError("Cycle found", group_path)
                print(' << cycle found: %r' % (group_path,))
                looped = True

            subjects = group_to_subjects[group]

            for subject in subjects:
                result.append(subject)
                if not looped and subject in group_to_subjects:
                    subsubjects = walk_group_to_subjects(subject, parents=group_path)
                    result.extend(subsubjects)

            if not looped:  # (otherwise the result is incomplete and shouldn't be saved)
                print(' << memo: %r <- %r' % (group, result))
                group_to_subjects_memo[group] = result

            return result

        return {
            group: set(walk_group_to_subjects(group))
            for group in group_to_subjects}


@pytest.mark.skip('broken, not very relevant to the current code')
def test_main():
    worker = Worker()
    worker.log('')
    worker.run_actions((
        ('group_add_subject', 'group0', 'group1'),
        ('group_add_subject', 'group1', 'user1'),
        ('group_add_subject', 'group2', 'group1'),
        ('group_add_subject', 'group1', 'group2'),
        ('group_add_subject', 'group2', 'group2'),
        ('group_add_subject', 'group0', 'user1'),
    ))
    worker.log('user1 groups: %r', worker.get_subject_effective_group_memberships('user1'))
    worker.log('group1 groups: %r', worker.get_subject_effective_group_memberships('group1'))
    worker.run_actions((
        ('group_add_subject', 'group2', 'user2'),
    ))
    worker.log('user2 groups: %r', worker.get_subject_effective_group_memberships('user2'))
    worker.run_actions((
        ('group_del_subject', 'group0', 'user1'),
        ('group_del_subject', 'group1', 'user1'),
    ))
    worker.log('user1 groups: %r', worker.get_subject_effective_group_memberships('user1'))


class Count:
    """ itertools.count that exposes the last value """

    last_value = None

    def __init__(self, start=0):
        self.start = start

    def __iter__(self):
        current = self.start
        while True:
            self.last_value = current
            yield current
            current += 1


def weighted_choice_prepare(choices):
    weight_norm = sum(weight for weight, _ in choices)
    return [
        (weight / weight_norm, choice)
        for weight, choice in choices]


def weighted_choice(rnd, choices):
    """
    ...

    >>> from collections import Counter
    >>> rnd = base_random.Random(2)
    >>> choices = ((0.9, "au"), (0.6, "ag"), (0.0001, "dm"))
    >>> choices = weighted_choice_prepare(choices);
    >>> result = Counter(weighted_choice(rnd, choices) for _ in range(10000))
    >>> result
    Counter({'au': 6047, 'ag': 3952, 'dm': 1})
    """
    cutoff = rnd.random()
    current = 0
    choice = None
    for weight, choice in choices:
        current += weight
        if current >= cutoff:
            return choice
    return choice


@pytest.mark.skip('broken, not very relevant to the current code')
def test_random(steps=100000, with_check_the_world=True, disable_log=True):
    worker = Worker()

    if disable_log:
        worker.log_enable = False

    worker.log('')
    rnd = base_random.Random(1)

    user_idx_gen = Count(1)
    user_gen = ("user{}".format(idx) for idx in user_idx_gen)
    group_idx_gen = Count(1)
    group_gen = ("group{}".format(idx) for idx in group_idx_gen)

    def random_user(with_new=0):
        max_id = user_idx_gen.last_value
        if not max_id or rnd.random() < with_new:
            return next(user_gen)
        return "user{}".format(rnd.randrange(max_id) + 1)

    def random_group(with_new=0):
        max_id = group_idx_gen.last_value
        if not max_id or rnd.random() < with_new:
            return next(group_gen)
        return "group{}".format(rnd.randrange(max_id) + 1)

    def random_direct_membership():
        if not worker.direct_memberships:
            return None
        return rnd.choice(sorted(worker.direct_memberships))

    def delete_membership(membership):
        if not membership:
            return []
        assert membership.group == membership.reason
        return worker.group_del_subject(membership.group, membership.subject)

    actions = (
        (0.9, lambda: ('group_add_subject', random_group(with_new=0.01), random_user(with_new=0.01))),
        (0.6, lambda: ('group_add_subject', random_group(with_new=0.01), random_group(with_new=0.01))),
        (0.0001, lambda: (delete_membership, random_direct_membership())),
    )
    actions_choices = weighted_choice_prepare(actions)

    for step in range(steps):
        action_gen = weighted_choice(rnd, actions_choices)
        action = action_gen()
        action_func, action_args = action[0], action[1:]
        title = action

        print(" ======= step=%r, action=%r, direct_len=%r, effective_len=%r:" % (
            step, title, len(worker.direct_memberships), len(worker.effective_memberships)))

        action_func(*action_args)

        if with_check_the_world:
            worker.check_the_world()


if __name__ == '__main__':
    test_main()
