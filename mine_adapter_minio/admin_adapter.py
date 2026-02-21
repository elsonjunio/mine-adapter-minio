import subprocess
import json
from typing import Dict, List


from mine_spec.dto.admin import (
    GroupMembership,
    User,
    GroupInfo,
    GroupList,
    GroupMappings,
    ResultGroupMappings,
    GroupPolicyMapp,
    GroupPolicyDetached,
    GroupPolicyAttached,
    BucketQuota,
    ListServiceAccounts,
    CreateServiceAccount,
    PolicyInfo,
    Policy,
    PolicyAttached,
    PolicyDetached,
)

from mine_spec.ports.admin import UserAdminPort


def _map_user(raw: dict) -> User:
    groups = []

    for group in raw.get('memberOf', []):
        groups.append(
            GroupMembership(
                name=group['name'],
                policies=group.get('policies'),
            )
        )

    return User(
        access_key=raw['accessKey'],
        status=raw['userStatus'] if 'userStatus' in raw else '',
        member_of=groups,
    )


def _map_group_info(raw: dict) -> GroupInfo:
    return GroupInfo(
        group_name=raw['groupName'],
        members=raw['members'] if 'members' in raw else [],
        status=raw['groupStatus'] if 'groupStatus' in raw else '',
    )


def _map_group_policy(raw: dict) -> GroupPolicyMapp:
    group_mappings = []

    for group in raw['result'].get('groupMappings', []):
        group_mappings.append(
            GroupMappings(
                group=group['group'],
                policies=group.get('policies'),
            )
        )

    result_group_mappings = ResultGroupMappings(
        timestamp=raw['result']['timestamp'], group_mappings=group_mappings
    )

    return GroupPolicyMapp(result=result_group_mappings)


def _map_policy(raw: dict) -> Policy:
    policy_info = PolicyInfo(
        policy_name=raw['policyInfo']['policyName']
        if 'policyName' in raw['policyInfo']
        else '',
        policy=raw['policyInfo']['Policy']
        if 'Policy' in raw['policyInfo']
        else None,
        create_date=raw['policyInfo']['createDate']
        if 'createDate' in raw['policyInfo']
        else '',
        update_date=raw['policyInfo']['updateDate']
        if 'updateDate' in raw['policyInfo']
        else '',
    )

    policy = Policy(
        policy=raw['policy'], policy_info=policy_info, is_group=raw['isGroup']
    )

    return policy


class MinioAdminAdapter(UserAdminPort):
    def __init__(
        self,
        alias,
        endpoint,
        access_key,
        secret_key,
        secure,
    ):
        self.alias = alias
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure

    # --------------------------------------------------------
    # Setup alias
    # --------------------------------------------------------
    def setup(self):
        """
        Configura alias do mc.
        Deve ser executado no startup da aplicação.
        """

        protocol = 'https' if self.secure else 'http'

        result = subprocess.run(
            [
                'mc',
                'alias',
                'set',
                self.alias,
                f'{protocol}://{self.endpoint}',
                self.access_key,
                self.secret_key,
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f'Failed to configure mc alias: {result.stderr}'
            )

    # --------------------------------------------------------
    # Execute generic mc command
    # --------------------------------------------------------
    def run(self, *args):
        """
        Executa comando mc com saída JSON.
        """

        command = ['mc', '--json', *args]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            error = result.stderr if result.stderr else result.stdout
            raise RuntimeError(error)

        # mc pode retornar múltiplas linhas JSON
        lines = result.stdout.strip().split('\n')
        parsed = [json.loads(line) for line in lines if line.strip()]

        return parsed

    # --------------------------------------------------------
    # Set bucket quota
    # --------------------------------------------------------
    def set_bucket_quota(self, bucket: str, quota: str) -> List[BucketQuota]:
        """
        quota example: '10GiB'
        """

        raw = self.run(
            'quota',
            'set',
            f'{self.alias}/{bucket}',
            '--size',
            quota,
        )

        return [
            BucketQuota(
                bucket=item['bucket'],
                quota_bytes=item['quota'],
                type=item['type'],
            )
            for item in raw
        ]

    # --------------------------------------------------------
    # Info about bucket quota
    # --------------------------------------------------------

    def get_bucket_quota(self, bucket: str) -> List[BucketQuota]:
        raw = self.run('quota', 'info', f'{self.alias}/{bucket}')
        return [
            BucketQuota(
                bucket=item['bucket'],
                quota_bytes=item['quota'],
                type=item['type'],
            )
            for item in raw
        ]

    # --------------------------------------------------------
    # List users
    # --------------------------------------------------------

    def list_users(self) -> List[User]:
        raw_users = self.run(
            'admin',
            'user',
            'list',
            self.alias,
        )

        return [_map_user(user) for user in raw_users]

    # --------------------------------------------------------
    # User info
    # --------------------------------------------------------

    def get_user(self, username: str) -> List[User]:
        raw_users = self.run(
            'admin',
            'user',
            'info',
            self.alias,
            username,
        )
        return [_map_user(user) for user in raw_users]

    # --------------------------------------------------------
    # Create user
    # --------------------------------------------------------
    def create_user(self, username: str, password: str) -> List[User]:
        raw_user = self.run(
            'admin',
            'user',
            'add',
            self.alias,
            username,
            password,
        )
        return [_map_user(user) for user in raw_user]

    # --------------------------------------------------------
    # Delete user
    # --------------------------------------------------------
    def delete_user(self, username: str) -> List[User]:
        raw_user = self.run(
            'admin',
            'user',
            'remove',
            self.alias,
            username,
        )

        return [_map_user(user) for user in raw_user]

    # --------------------------------------------------------
    # Enable user
    # --------------------------------------------------------
    def enable_user(self, username: str) -> List[User]:
        raw_user = self.run(
            'admin',
            'user',
            'enable',
            self.alias,
            username,
        )
        return [_map_user(user) for user in raw_user]

    # --------------------------------------------------------
    # Disable user
    # --------------------------------------------------------
    def disable_user(self, username: str) -> List[User]:
        raw_user = self.run(
            'admin',
            'user',
            'disable',
            self.alias,
            username,
        )

        return [_map_user(user) for user in raw_user]

    # --------------------------------------------------------
    # GROUP - LIST
    # --------------------------------------------------------
    def list_groups(self) -> List[GroupList]:
        groups = self.run(
            'admin',
            'group',
            'list',
            self.alias,
        )

        return [GroupList(groups=group['groups']) for group in groups]

    # --------------------------------------------------------
    # GROUP - INFO
    # --------------------------------------------------------
    def group_info(self, name: str) -> List[GroupInfo]:
        raw_group = self.run(
            'admin',
            'group',
            'info',
            self.alias,
            name,
        )

        return [_map_group_info(group) for group in raw_group]

    # --------------------------------------------------------
    # GROUP - CREATE / ADD USERS
    # --------------------------------------------------------
    def create_group(self, name: str, users: list[str]) -> List[GroupInfo]:
        raw_group = self.run(
            'admin',
            'group',
            'add',
            self.alias,
            name,
            *users,
        )

        return [_map_group_info(group) for group in raw_group]

    # --------------------------------------------------------
    # GROUP - REMOVE GROUP
    # --------------------------------------------------------
    def remove_group(self, name: str) -> List[GroupInfo]:
        raw_group = self.run(
            'admin',
            'group',
            'remove',
            self.alias,
            name,
        )

        return [_map_group_info(group) for group in raw_group]

    # --------------------------------------------------------
    # GROUP - REMOVE USERS
    # --------------------------------------------------------
    def remove_users_from_group(
        self, name: str, users: list[str]
    ) -> List[GroupInfo]:
        raw_group = self.run(
            'admin',
            'group',
            'remove',
            self.alias,
            name,
            *users,
        )

        return [_map_group_info(group) for group in raw_group]

    # --------------------------------------------------------
    # GROUP - CREATE / ADD USERS
    # --------------------------------------------------------
    def add_users_to_group(
        self, name: str, users: list[str]
    ) -> List[GroupInfo]:
        raw_group = self.run(
            'admin',
            'group',
            'add',
            self.alias,
            name,
            *users,
        )

        return [_map_group_info(group) for group in raw_group]

    # --------------------------------------------------------
    # GROUP - ENABLE
    # --------------------------------------------------------
    def enable_group(self, name: str) -> List[GroupInfo]:
        raw_group = self.run(
            'admin',
            'group',
            'enable',
            self.alias,
            name,
        )

        return [_map_group_info(group) for group in raw_group]

    # --------------------------------------------------------
    # GROUP - DISABLE
    # --------------------------------------------------------
    def disable_group(self, name: str) -> List[GroupInfo]:
        raw_group = self.run(
            'admin',
            'group',
            'disable',
            self.alias,
            name,
        )
        return [_map_group_info(group) for group in raw_group]

    # --------------------------------------------------------
    # GROUP - ATTACH POLICY
    # --------------------------------------------------------
    def attach_policy_to_group(
        self, policy: str, group: str
    ) -> List[GroupPolicyAttached]:
        raw_group_policy = self.run(
            'admin',
            'policy',
            'attach',
            self.alias,
            policy,
            '--group',
            group,
        )
        return [
            GroupPolicyAttached(
                group=group_policy['group'],
                policies_attached=group_policy['policiesAttached'],
            )
            for group_policy in raw_group_policy
        ]

    # --------------------------------------------------------
    # GROUP - DETACH POLICY
    # --------------------------------------------------------
    def detach_policy_from_group(
        self, policy: str, group: str
    ) -> List[GroupPolicyDetached]:
        raw_group_policy = self.run(
            'admin',
            'policy',
            'detach',
            self.alias,
            policy,
            '--group',
            group,
        )
        return [
            GroupPolicyDetached(
                group=group_policy['group'],
                policies_detached=group_policy['policiesDetached'],
            )
            for group_policy in raw_group_policy
        ]

    # --------------------------------------------------------
    # GROUP - LIST ATTACHED POLICY
    # --------------------------------------------------------
    def get_policy_from_group(self, group: str) -> List[GroupPolicyMapp]:
        raw_group_policy = self.run(
            'admin', 'policy', 'entities', '--group', group, self.alias
        )

        return [
            _map_group_policy(group_policy)
            for group_policy in raw_group_policy
        ]

    # --------------------------------------------------------
    # List service accounts for a user
    # --------------------------------------------------------
    def list_service_accounts(
        self, username: str
    ) -> List[ListServiceAccounts]:
        raw = self.run(
            'admin',
            'user',
            'svcacct',
            'list',
            self.alias,
            username,
        )

        return [
            ListServiceAccounts(access_key=item['accessKey']) for item in raw
        ]

    # --------------------------------------------------------
    # Create service account
    # --------------------------------------------------------
    def create_service_account(
        self,
        username: str,
        policy: str | None = None,
        expiration: str | None = None,
    ) -> List[CreateServiceAccount]:
        command = [
            'admin',
            'user',
            'svcacct',
            'add',
            self.alias,
            username,
        ]

        if policy:
            command.extend(['--policy', policy])

        if expiration:
            command.extend(['--expiry', expiration])

        raw = self.run(*command)

        return [
            CreateServiceAccount(
                status=item['accountStatus'],
                access_key=item['accessKey'],
                secret_key=item['secretKey'],
                expiration=item['expiration'],
            )
            for item in raw
        ]

    # --------------------------------------------------------
    # Delete service account
    # --------------------------------------------------------
    def delete_service_account(
        self, access_key: str
    ) -> List[ListServiceAccounts]:
        raw = self.run(
            'admin',
            'user',
            'svcacct',
            'remove',
            self.alias,
            access_key,
        )

        return [
            ListServiceAccounts(access_key=item['accessKey']) for item in raw
        ]

    # --------------------------------------------------------
    # List policies
    # --------------------------------------------------------
    def list_policies(self) -> List[Policy]:
        raw = self.run(
            'admin',
            'policy',
            'list',
            self.alias,
        )

        return [_map_policy(item) for item in raw]

    # --------------------------------------------------------
    # Get policy info
    # --------------------------------------------------------
    def get_policy(self, name: str) -> List[Policy]:
        raw = self.run(
            'admin',
            'policy',
            'info',
            self.alias,
            name,
        )
        return [_map_policy(item) for item in raw]

    # --------------------------------------------------------
    # Create policy
    # --------------------------------------------------------
    def create_policy(self, name: str, file_path: str) -> List[Policy]:
        raw = self.run(
            'admin',
            'policy',
            'create',
            self.alias,
            name,
            file_path,
        )
        return [_map_policy(item) for item in raw]

    # --------------------------------------------------------
    # Delete policy
    # --------------------------------------------------------
    def delete_policy(self, name: str) -> List[Policy]:
        raw = self.run(
            'admin',
            'policy',
            'remove',
            self.alias,
            name,
        )

        return [_map_policy(item) for item in raw]

    # --------------------------------------------------------
    # Attach policy to user
    # --------------------------------------------------------
    def attach_policy(
        self, policy: str, username: str
    ) -> List[PolicyAttached]:
        raw = self.run(
            'admin',
            'policy',
            'attach',
            self.alias,
            policy,
            '--user',
            username,
        )

        return [
            PolicyAttached(
                policies_attached=item['policiesAttached'], user=item['user']
            )
            for item in raw
        ]

    # --------------------------------------------------------
    # Detach policy from user
    # --------------------------------------------------------
    def detach_policy(
        self, policy: str, username: str
    ) -> List[PolicyDetached]:
        raw = self.run(
            'admin',
            'policy',
            'detach',
            self.alias,
            policy,
            '--user',
            username,
        )

        return [
            PolicyDetached(
                policies_detached=item['policiesDetached'], user=item['user']
            )
            for item in raw
        ]

    def add_notification_target(
        self,
        target_type: str,
        identifier: str,
        config: dict,
    ) -> List[Dict]:
        """
        target_type: webhook, kafka, amqp, etc
        identifier: nome lógico
        config: dict com parâmetros do target
        """

        key = f'notify_{target_type}:{identifier}'

        args = [
            'admin',
            'config',
            'set',
            self.alias,
            key,
        ]

        for k, v in config.items():
            args.append(f'{k}={v}')

        result = self.run(*args)

        # aplicar config
        self.run('admin', 'service', 'restart', self.alias)

        return result

    def remove_notification_target(
        self,
        target_type: str,
        identifier: str,
    ) -> List[Dict]:
        key = f'notify_{target_type}:{identifier}'

        self.run('admin', 'config', 'reset', self.alias, key)

        self.run('admin', 'service', 'restart', self.alias)

        return [{'message': 'Notification target disabled'}]

    def list_notification_targets(
        self, target_type: str | None = None
    ) -> List[Dict]:
        """
        Se target_type for None, lista todos.
        """

        if target_type:
            key = f'notify_{target_type}'
        else:
            key = 'notify'

        return self.run(
            'admin',
            'config',
            'get',
            self.alias,
            key,
        )
