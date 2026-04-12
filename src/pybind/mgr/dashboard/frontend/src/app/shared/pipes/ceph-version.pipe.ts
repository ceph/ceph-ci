import { Pipe, PipeTransform } from '@angular/core';
import { getVersionAndRelease } from '../helpers/utils';

@Pipe({
  name: 'cephVersion',
  standalone: false
})
export class CephVersionPipe implements PipeTransform {
  transform(value: string = ''): string {
    // Expect ""ceph version 20.2.1-102.0.TEST.wip_rocky10_branch_of_the_day_9.1_20260408_1600.el10cp
    //          (2cabc0155d4201501d3d2a098f9c80781fd8f293) tentacle (stable - RelWithDebInfo) release 9.1.0"
    if (value) {
      return getVersionAndRelease(value)?.release;
    }
    return value;
  }
}
