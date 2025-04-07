import { Component, OnDestroy, OnInit } from '@angular/core';
import { BaseModal } from 'carbon-components-angular';
import { detect } from 'detect-browser';
import { Subscription } from 'rxjs';
import { UserService } from '~/app/shared/api/user.service';
import { AppConstants, USER } from '~/app/shared/constants/app.constants';
import { LocalStorage } from '~/app/shared/enum/local-storage-enum';
import { getVersionAndRelease } from '~/app/shared/helpers/utils';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { SummaryService } from '~/app/shared/services/summary.service';

@Component({
  selector: 'cd-about',
  templateUrl: './about.component.html',
  styleUrls: ['./about.component.scss']
})
export class AboutComponent extends BaseModal implements OnInit, OnDestroy {
  modalVariables: any;
  subs: Subscription;
  userPermission: Permission;
  projectConstants: typeof AppConstants;
  hostAddr: string;
  copyright: string;
  version: string;
  release: string;

  constructor(
    private summaryService: SummaryService,
    private userService: UserService,
    private authStorageService: AuthStorageService
  ) {
    super();
    this.userPermission = this.authStorageService.getPermissions().user;
  }

  ngOnInit() {
    this.projectConstants = AppConstants;
    this.hostAddr = window.location.hostname;
    this.modalVariables = this.setVariables();
    this.subs = this.summaryService.subscribe((summary) => {
      const {release, version} = getVersionAndRelease(summary.version);
      this.release = release;
      this.version = version.split(' ')[0];
      this.hostAddr = summary.mgr_host.replace(/(^\w+:|^)\/\//, '').replace(/\/$/, '');
    });
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  setVariables() {
    const project = {} as any;
    project.user = localStorage.getItem(LocalStorage.DASHBOARD_USRENAME);
    project.role = USER;
    if (this.userPermission.read) {
      this.userService.get(project.user).subscribe((data: any) => {
        project.role = data.roles;
      });
    }
    const browser = detect();
    project.browserName = browser && browser.name ? browser.name : 'Not detected';
    project.browserVersion = browser && browser.version ? browser.version : 'Not detected';
    project.browserOS = browser && browser.os ? browser.os : 'Not detected';
    return project;
  }
}
