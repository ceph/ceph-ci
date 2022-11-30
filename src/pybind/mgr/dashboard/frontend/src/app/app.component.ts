import { DOCUMENT } from '@angular/common';
import { Component, Inject } from '@angular/core';

import { NgbPopoverConfig, NgbTooltipConfig } from '@ng-bootstrap/ng-bootstrap';

import { environment } from '~/environments/environment';

@Component({
  selector: 'cd-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent {
  constructor(
    popoverConfig: NgbPopoverConfig,
    tooltipConfig: NgbTooltipConfig,
    @Inject(DOCUMENT) private document: Document
  ) {
    popoverConfig.autoClose = 'outside';
    popoverConfig.container = 'body';
    popoverConfig.placement = 'bottom';

    tooltipConfig.container = 'body';

    const favicon = this.document.getElementById('cdFavicon');
    let projectName = '';

    if (environment.build === 'redhat') {
      const headEl = this.document.getElementsByTagName('head')[0];
      const newLinkEl = this.document.createElement('link');
      newLinkEl.rel = 'stylesheet';
      newLinkEl.href = 'rh-overrides.css';

      headEl.appendChild(newLinkEl);
      projectName = 'Red Hat Ceph Storage';
      favicon.setAttribute('href', 'assets/RedHat_favicon_0319.svg');
    } else if (environment.build === 'ibm') {

      projectName = 'IBM Storage Ceph';
      favicon.setAttribute('href', 'assets/StorageCeph_favicon.svg');
    }
    this.document.title = projectName;
  }
}
