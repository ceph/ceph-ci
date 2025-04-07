export function getVersionAndRelease(input: string): { version: string ; release: string } {
    if (!input.startsWith('ceph version')) return {version: '', release: ''};
    const res = input.replace('ceph version', '').trim().split(' release ');
    return {
      release: res[1].trim(),
      version: res[0].trim(),
  }
}
